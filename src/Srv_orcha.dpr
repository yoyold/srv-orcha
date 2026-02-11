unit Srv_orcha;

interface

uses
  Windows, Messages, SysUtils, Classes, SvcMgr, SyncObjs, Generics.Collections,
  System.Threading, System.TimeSpan;

type
  // Service status enumeration
  TServiceStatus = (ssStarting, ssRunning, ssStopping, ssStopped, ssError);

  // Configuration for a managed service
  TServiceConfig = class
  public
    Name: string;
    DisplayName: string;
    ExecutablePath: string;
    Arguments: string;
    WorkingDirectory: string;
    RestartOnFailure: Boolean;
    MaxRestartAttempts: Integer;
    RestartDelaySeconds: Integer;
    DependsOn: TArray<string>;
    Enabled: Boolean;

    constructor Create;
  end;

  // Managed service instance
  TManagedService = class
  private
    FConfig: TServiceConfig;
    FProcessInfo: TProcessInformation;
    FStatus: TServiceStatus;
    FRestartAttempts: Integer;
    FLastStartTime: TDateTime;
    FMonitorThread: TThread;
    FStopEvent: TEvent;

    procedure StartMonitoring;
    procedure StopMonitoring;
    function IsProcessRunning: Boolean;
    procedure RestartService;

  public
    constructor Create(AConfig: TServiceConfig);
    destructor Destroy; override;

    function Start: Boolean;
    procedure Stop;
    procedure Kill;

    property Config: TServiceConfig read FConfig;
    property Status: TServiceStatus read FStatus;
    property ProcessId: DWORD read FProcessInfo.dwProcessId;
  end;

  // Main orchestrator service
  TServiceOrchestrator = class(TService)
  private
    FServices: TObjectDictionary<string, TManagedService>;
    FConfigFile: string;
    FLogFile: string;
    FCriticalSection: TCriticalSection;
    FShutdownEvent: TEvent;
    FMonitorThread: TThread;

    procedure LoadConfiguration;
    procedure SaveConfiguration;
    procedure StartAllServices;
    procedure StopAllServices;
    procedure MonitorServices;
    procedure LogMessage(const Msg: string);
    function ResolveDependencies: TArray<string>;

  protected
    function GetServiceController: TServiceController; override;
    procedure ServiceStart(Sender: TService; var Started: Boolean);
    procedure ServiceStop(Sender: TService; var Stopped: Boolean);
    procedure ServicePause(Sender: TService; var Paused: Boolean);
    procedure ServiceContinue(Sender: TService; var Continued: Boolean);

  public
    constructor CreateNew(AOwner: TComponent); override;
    destructor Destroy; override;

    // Management methods
    function AddService(const AName, ADisplayName, AExecutablePath: string;
      const AArguments: string = ''; const AWorkingDir: string = ''): Boolean;
    function RemoveService(const AName: string): Boolean;
    function StartService(const AName: string): Boolean;
    function StopService(const AName: string): Boolean;
    function GetServiceStatus(const AName: string): TServiceStatus;
    procedure SetServiceEnabled(const AName: string; AEnabled: Boolean);

  end;

var
  ServiceOrchestrator: TServiceOrchestrator;

implementation

uses
  IniFiles, DateUtils;

{ TServiceConfig }

constructor TServiceConfig.Create;
begin
  inherited;
  RestartOnFailure := True;
  MaxRestartAttempts := 3;
  RestartDelaySeconds := 10;
  Enabled := True;
  SetLength(DependsOn, 0);
end;

{ TManagedService }

constructor TManagedService.Create(AConfig: TServiceConfig);
begin
  inherited Create;
  FConfig := AConfig;
  FStatus := ssStopped;
  FRestartAttempts := 0;
  FStopEvent := TEvent.Create(nil, True, False, '');
  ZeroMemory(@FProcessInfo, SizeOf(FProcessInfo));
end;

destructor TManagedService.Destroy;
begin
  Stop;
  FStopEvent.Free;
  FConfig.Free;
  inherited;
end;

function TManagedService.Start: Boolean;
var
  StartupInfo: TStartupInfo;
  CommandLine: string;
  WorkDir: PChar;
begin
  Result := False;

  if FStatus = ssRunning then
    Exit(True);

  FStatus := ssStarting;

  try
    ZeroMemory(@StartupInfo, SizeOf(StartupInfo));
    StartupInfo.cb := SizeOf(StartupInfo);
    StartupInfo.dwFlags := STARTF_USESHOWWINDOW;
    StartupInfo.wShowWindow := SW_HIDE;

    CommandLine := Format('"%s" %s', [FConfig.ExecutablePath, FConfig.Arguments]);

    WorkDir := nil;
    if FConfig.WorkingDirectory <> '' then
      WorkDir := PChar(FConfig.WorkingDirectory);

    if CreateProcess(nil, PChar(CommandLine), nil, nil, False,
                     CREATE_NEW_CONSOLE, nil, WorkDir, StartupInfo, FProcessInfo) then
    begin
      FStatus := ssRunning;
      FLastStartTime := Now;
      StartMonitoring;
      Result := True;
    end
    else
    begin
      FStatus := ssError;
      raise Exception.CreateFmt('Failed to start service %s: %s',
        [FConfig.Name, SysErrorMessage(GetLastError)]);
    end;

  except
    on E: Exception do
    begin
      FStatus := ssError;
      // Log error
    end;
  end;
end;

procedure TManagedService.Stop;
begin
  if FStatus <> ssRunning then
    Exit;

  FStatus := ssStopping;
  StopMonitoring;

  if FProcessInfo.hProcess <> 0 then
  begin
    // Try graceful shutdown first
    if not TerminateProcess(FProcessInfo.hProcess, 0) then
    begin
      // Force kill if graceful shutdown fails
      Kill;
    end;

    CloseHandle(FProcessInfo.hProcess);
    CloseHandle(FProcessInfo.hThread);
    ZeroMemory(@FProcessInfo, SizeOf(FProcessInfo));
  end;

  FStatus := ssStopped;
end;

procedure TManagedService.Kill;
begin
  if FProcessInfo.hProcess <> 0 then
  begin
    TerminateProcess(FProcessInfo.hProcess, 1);
    FStatus := ssStopped;
  end;
end;

function TManagedService.IsProcessRunning: Boolean;
var
  ExitCode: DWORD;
begin
  Result := False;

  if FProcessInfo.hProcess = 0 then
    Exit;

  if GetExitCodeProcess(FProcessInfo.hProcess, ExitCode) then
    Result := (ExitCode = STILL_ACTIVE);
end;

procedure TManagedService.StartMonitoring;
begin
  if FMonitorThread <> nil then
    Exit;

  FStopEvent.ResetEvent;

  FMonitorThread := TThread.CreateAnonymousThread(
    procedure
    begin
      while not FStopEvent.WaitFor(1000) = wrSignaled do
      begin
        if not IsProcessRunning then
        begin
          FStatus := ssError;

          if FConfig.RestartOnFailure and
             (FRestartAttempts < FConfig.MaxRestartAttempts) then
          begin
            Inc(FRestartAttempts);
            Sleep(FConfig.RestartDelaySeconds * 1000);

            if FStopEvent.WaitFor(0) <> wrSignaled then
              RestartService;
          end
          else
          begin
            FStatus := ssStopped;
            Break;
          end;
        end;
      end;
    end);

  FMonitorThread.Start;
end;

procedure TManagedService.StopMonitoring;
begin
  if FMonitorThread = nil then
    Exit;

  FStopEvent.SetEvent;
  FMonitorThread.WaitFor;
  FreeAndNil(FMonitorThread);
end;

procedure TManagedService.RestartService;
begin
  Stop;
  Sleep(1000); // Brief pause before restart
  Start;
end;

{ TServiceOrchestrator }

constructor TServiceOrchestrator.CreateNew(AOwner: TComponent);
begin
  inherited;

  Name := 'ServiceOrchestrator';
  DisplayName := 'Service Orchestrator';

  FServices := TObjectDictionary<string, TManagedService>.Create([doOwnsValues]);
  FCriticalSection := TCriticalSection.Create;
  FShutdownEvent := TEvent.Create(nil, True, False, '');

  FConfigFile := ExtractFilePath(ParamStr(0)) + 'ServiceOrchestrator.ini';
  FLogFile := ExtractFilePath(ParamStr(0)) + 'ServiceOrchestrator.log';
end;

destructor TServiceOrchestrator.Destroy;
begin
  StopAllServices;
  FShutdownEvent.Free;
  FCriticalSection.Free;
  FServices.Free;
  inherited;
end;

function TServiceOrchestrator.GetServiceController: TServiceController;
begin
  Result := ServiceController;
end;

procedure TServiceOrchestrator.ServiceStart(Sender: TService; var Started: Boolean);
begin
  try
    LogMessage('Service Orchestrator starting...');
    LoadConfiguration;
    StartAllServices;

    // Start monitoring thread
    FMonitorThread := TThread.CreateAnonymousThread(
      procedure
      begin
        MonitorServices;
      end);
    FMonitorThread.Start;

    Started := True;
    LogMessage('Service Orchestrator started successfully');

  except
    on E: Exception do
    begin
      LogMessage('Failed to start Service Orchestrator: ' + E.Message);
      Started := False;
    end;
  end;
end;

procedure TServiceOrchestrator.ServiceStop(Sender: TService; var Stopped: Boolean);
begin
  try
    LogMessage('Service Orchestrator stopping...');

    FShutdownEvent.SetEvent;

    if Assigned(FMonitorThread) then
    begin
      FMonitorThread.WaitFor;
      FreeAndNil(FMonitorThread);
    end;

    StopAllServices;
    SaveConfiguration;

    Stopped := True;
    LogMessage('Service Orchestrator stopped successfully');

  except
    on E: Exception do
    begin
      LogMessage('Error stopping Service Orchestrator: ' + E.Message);
      Stopped := True; // Force stop even on error
    end;
  end;
end;

procedure TServiceOrchestrator.ServicePause(Sender: TService; var Paused: Boolean);
begin
  // Pause all running services
  FCriticalSection.Enter;
  try
    for var Service in FServices.Values do
    begin
      if Service.Status = ssRunning then
        Service.Stop;
    end;
    Paused := True;
  finally
    FCriticalSection.Leave;
  end;
end;

procedure TServiceOrchestrator.ServiceContinue(Sender: TService; var Continued: Boolean);
begin
  // Resume all enabled services
  StartAllServices;
  Continued := True;
end;

procedure TServiceOrchestrator.LoadConfiguration;
var
  IniFile: TIniFile;
  Sections: TStringList;
  I: Integer;
  ServiceName: string;
  Config: TServiceConfig;
  Service: TManagedService;
begin
  if not FileExists(FConfigFile) then
    Exit;

  IniFile := TIniFile.Create(FConfigFile);
  Sections := TStringList.Create;

  try
    IniFile.ReadSections(Sections);

    for I := 0 to Sections.Count - 1 do
    begin
      ServiceName := Sections[I];

      if ServiceName = 'GENERAL' then
        Continue;

      Config := TServiceConfig.Create;
      Config.Name := ServiceName;
      Config.DisplayName := IniFile.ReadString(ServiceName, 'DisplayName', ServiceName);
      Config.ExecutablePath := IniFile.ReadString(ServiceName, 'ExecutablePath', '');
      Config.Arguments := IniFile.ReadString(ServiceName, 'Arguments', '');
      Config.WorkingDirectory := IniFile.ReadString(ServiceName, 'WorkingDirectory', '');
      Config.RestartOnFailure := IniFile.ReadBool(ServiceName, 'RestartOnFailure', True);
      Config.MaxRestartAttempts := IniFile.ReadInteger(ServiceName, 'MaxRestartAttempts', 3);
      Config.RestartDelaySeconds := IniFile.ReadInteger(ServiceName, 'RestartDelaySeconds', 10);
      Config.Enabled := IniFile.ReadBool(ServiceName, 'Enabled', True);

      if Config.ExecutablePath <> '' then
      begin
        Service := TManagedService.Create(Config);
        FServices.Add(ServiceName, Service);
      end
      else
        Config.Free;
    end;

  finally
    Sections.Free;
    IniFile.Free;
  end;
end;

procedure TServiceOrchestrator.SaveConfiguration;
var
  IniFile: TIniFile;
  ServiceName: string;
  Service: TManagedService;
begin
  IniFile := TIniFile.Create(FConfigFile);

  try
    // Clear existing sections
    var Sections := TStringList.Create;
    try
      IniFile.ReadSections(Sections);
      for var Section in Sections do
        if Section <> 'GENERAL' then
          IniFile.EraseSection(Section);
    finally
      Sections.Free;
    end;

    // Write current configuration
    for ServiceName in FServices.Keys do
    begin
      Service := FServices[ServiceName];

      IniFile.WriteString(ServiceName, 'DisplayName', Service.Config.DisplayName);
      IniFile.WriteString(ServiceName, 'ExecutablePath', Service.Config.ExecutablePath);
      IniFile.WriteString(ServiceName, 'Arguments', Service.Config.Arguments);
      IniFile.WriteString(ServiceName, 'WorkingDirectory', Service.Config.WorkingDirectory);
      IniFile.WriteBool(ServiceName, 'RestartOnFailure', Service.Config.RestartOnFailure);
      IniFile.WriteInteger(ServiceName, 'MaxRestartAttempts', Service.Config.MaxRestartAttempts);
      IniFile.WriteInteger(ServiceName, 'RestartDelaySeconds', Service.Config.RestartDelaySeconds);
      IniFile.WriteBool(ServiceName, 'Enabled', Service.Config.Enabled);
    end;

  finally
    IniFile.Free;
  end;
end;

procedure TServiceOrchestrator.StartAllServices;
var
  StartOrder: TArray<string>;
  ServiceName: string;
  Service: TManagedService;
begin
  StartOrder := ResolveDependencies;

  FCriticalSection.Enter;
  try
    for ServiceName in StartOrder do
    begin
      if FServices.TryGetValue(ServiceName, Service) and
         Service.Config.Enabled and
         (Service.Status <> ssRunning) then
      begin
        try
          LogMessage(Format('Starting service: %s', [ServiceName]));
          Service.Start;
        except
          on E: Exception do
            LogMessage(Format('Failed to start service %s: %s', [ServiceName, E.Message]));
        end;
      end;
    end;
  finally
    FCriticalSection.Leave;
  end;
end;

procedure TServiceOrchestrator.StopAllServices;
var
  ServiceName: string;
  Service: TManagedService;
begin
  FCriticalSection.Enter;
  try
    for ServiceName in FServices.Keys do
    begin
      Service := FServices[ServiceName];
      if Service.Status = ssRunning then
      begin
        LogMessage(Format('Stopping service: %s', [ServiceName]));
        Service.Stop;
      end;
    end;
  finally
    FCriticalSection.Leave;
  end;
end;

procedure TServiceOrchestrator.MonitorServices;
begin
  while FShutdownEvent.WaitFor(5000) <> wrSignaled do
  begin
    // Monitor services and log status
    FCriticalSection.Enter;
    try
      for var ServiceName in FServices.Keys do
      begin
        var Service := FServices[ServiceName];

        // Check if service should be running but isn't
        if Service.Config.Enabled and (Service.Status = ssStopped) then
        begin
          LogMessage(Format('Restarting stopped service: %s', [ServiceName]));
          Service.Start;
        end;
      end;
    finally
      FCriticalSection.Leave;
    end;
  end;
end;

procedure TServiceOrchestrator.LogMessage(const Msg: string);
var
  LogEntry: string;
  FileHandle: TextFile;
begin
  LogEntry := Format('[%s] %s', [FormatDateTime('yyyy-mm-dd hh:nn:ss', Now), Msg]);

  try
    AssignFile(FileHandle, FLogFile);
    if FileExists(FLogFile) then
      Append(FileHandle)
    else
      Rewrite(FileHandle);

    WriteLn(FileHandle, LogEntry);
    CloseFile(FileHandle);
  except
    // Ignore logging errors
  end;
end;

function TServiceOrchestrator.ResolveDependencies: TArray<string>;
var
  Result_List: TList<string>;
  Processed: TDictionary<string, Boolean>;

  procedure ProcessService(const ServiceName: string);
  var
    Service: TManagedService;
    Dependency: string;
  begin
    if Processed.ContainsKey(ServiceName) then
      Exit;

    if not FServices.TryGetValue(ServiceName, Service) then
      Exit;

    // Process dependencies first
    for Dependency in Service.Config.DependsOn do
      ProcessService(Dependency);

    Result_List.Add(ServiceName);
    Processed.Add(ServiceName, True);
  end;

begin
  Result_List := TList<string>.Create;
  Processed := TDictionary<string, Boolean>.Create;

  try
    for var ServiceName in FServices.Keys do
      ProcessService(ServiceName);

    Result := Result_List.ToArray;
  finally
    Processed.Free;
    Result_List.Free;
  end;
end;

// Management methods implementation

function TServiceOrchestrator.AddService(const AName, ADisplayName, AExecutablePath: string;
  const AArguments: string = ''; const AWorkingDir: string = ''): Boolean;
var
  Config: TServiceConfig;
  Service: TManagedService;
begin
  Result := False;

  if FServices.ContainsKey(AName) then
    Exit; // Service already exists

  Config := TServiceConfig.Create;
  Config.Name := AName;
  Config.DisplayName := ADisplayName;
  Config.ExecutablePath := AExecutablePath;
  Config.Arguments := AArguments;
  Config.WorkingDirectory := AWorkingDir;

  Service := TManagedService.Create(Config);

  FCriticalSection.Enter;
  try
    FServices.Add(AName, Service);
    SaveConfiguration;
    Result := True;
  finally
    FCriticalSection.Leave;
  end;
end;

function TServiceOrchestrator.RemoveService(const AName: string): Boolean;
var
  Service: TManagedService;
begin
  Result := False;

  FCriticalSection.Enter;
  try
    if FServices.TryGetValue(AName, Service) then
    begin
      Service.Stop;
      FServices.Remove(AName);
      SaveConfiguration;
      Result := True;
    end;
  finally
    FCriticalSection.Leave;
  end;
end;

function TServiceOrchestrator.StartService(const AName: string): Boolean;
var
  Service: TManagedService;
begin
  Result := False;

  FCriticalSection.Enter;
  try
    if FServices.TryGetValue(AName, Service) then
      Result := Service.Start;
  finally
    FCriticalSection.Leave;
  end;
end;

function TServiceOrchestrator.StopService(const AName: string): Boolean;
var
  Service: TManagedService;
begin
  Result := False;

  FCriticalSection.Enter;
  try
    if FServices.TryGetValue(AName, Service) then
    begin
      Service.Stop;
      Result := True;
    end;
  finally
    FCriticalSection.Leave;
  end;
end;

function TServiceOrchestrator.GetServiceStatus(const AName: string): TServiceStatus;
var
  Service: TManagedService;
begin
  Result := ssStopped;

  FCriticalSection.Enter;
  try
    if FServices.TryGetValue(AName, Service) then
      Result := Service.Status;
  finally
    FCriticalSection.Leave;
  end;
end;

procedure TServiceOrchestrator.SetServiceEnabled(const AName: string; AEnabled: Boolean);
var
  Service: TManagedService;
begin
  FCriticalSection.Enter;
  try
    if FServices.TryGetValue(AName, Service) then
    begin
      Service.Config.Enabled := AEnabled;

      if not AEnabled and (Service.Status = ssRunning) then
        Service.Stop;

      SaveConfiguration;
    end;
  finally
    FCriticalSection.Leave;
  end;
end;

end.

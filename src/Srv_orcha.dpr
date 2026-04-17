program Srv_orcha;

{$APPTYPE CONSOLE}

uses
  Winapi.Windows,
  Winapi.Messages,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  System.IniFiles,
  System.DateUtils,
  System.StrUtils,
  Vcl.SvcMgr;

{$R *.RES}

type
  TServiceStatus = (ssStarting, ssRunning, ssStopping, ssStopped, ssError);

  TLogProc = reference to procedure(const Msg: string);

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
    StopTimeoutSeconds: Integer;
    DependsOn: TArray<string>;
    Enabled: Boolean;
    constructor Create;
  end;

  TManagedService = class
  private
    FConfig: TServiceConfig;
    FProcessInfo: TProcessInformation;
    FStatus: TServiceStatus;
    FRestartAttempts: Integer;
    FLastStartTime: TDateTime;
    FMonitorThread: TThread;
    FStopEvent: TEvent;
    FLog: TLogProc;
    FChildLogHandle: THandle;
    function StartProcess: Boolean;
    procedure StartMonitoring;
    procedure StopMonitoring;
    function IsProcessRunning: Boolean;
    procedure Log(const Msg: string);
    function OpenChildLogHandle: THandle;
    procedure CloseChildHandles;
    function RequestGracefulShutdown(TimeoutMs: DWORD): Boolean;
  public
    constructor Create(AConfig: TServiceConfig; ALog: TLogProc);
    destructor Destroy; override;
    function Start: Boolean;
    procedure Stop;
    procedure Kill;
    property Config: TServiceConfig read FConfig;
    property Status: TServiceStatus read FStatus;
    property ProcessId: DWORD read FProcessInfo.dwProcessId;
  end;

  TServiceOrchestrator = class(TService)
  private
    FServices: TObjectDictionary<string, TManagedService>;
    FConfigFile: string;
    FLogFile: string;
    FCriticalSection: TCriticalSection;
    FLogLock: TCriticalSection;
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
    constructor CreateNew(AOwner: TComponent; Dummy: Integer = 0); override;
    destructor Destroy; override;
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

{ TServiceConfig }

constructor TServiceConfig.Create;
begin
  inherited;
  RestartOnFailure := True;
  MaxRestartAttempts := 3;
  RestartDelaySeconds := 10;
  StopTimeoutSeconds := 15;
  Enabled := True;
  SetLength(DependsOn, 0);
end;

{ EnumWindows helper used by TManagedService.RequestGracefulShutdown }

type
  PEnumCloseInfo = ^TEnumCloseInfo;
  TEnumCloseInfo = record
    PID: DWORD;
    Posted: Integer;
  end;

function EnumCloseProc(Wnd: HWND; lParam: LPARAM): BOOL; stdcall;
var
  WndPID: DWORD;
  Info: PEnumCloseInfo;
begin
  Result := True;
  WndPID := 0;
  GetWindowThreadProcessId(Wnd, WndPID);
  Info := PEnumCloseInfo(lParam);
  if WndPID = Info^.PID then
  begin
    PostMessage(Wnd, WM_CLOSE, 0, 0);
    Inc(Info^.Posted);
  end;
end;

{ TManagedService }

constructor TManagedService.Create(AConfig: TServiceConfig; ALog: TLogProc);
begin
  inherited Create;
  FConfig := AConfig;
  FLog := ALog;
  FStatus := ssStopped;
  FRestartAttempts := 0;
  FStopEvent := TEvent.Create(nil, True, False, '');
  FChildLogHandle := 0;
  ZeroMemory(@FProcessInfo, SizeOf(FProcessInfo));
end;

destructor TManagedService.Destroy;
begin
  try
    Stop;
  except
    on E: Exception do
      Log('Destroy/Stop error: ' + E.Message);
  end;
  CloseChildHandles;
  FStopEvent.Free;
  FConfig.Free;
  inherited;
end;

procedure TManagedService.Log(const Msg: string);
begin
  if Assigned(FLog) then
    FLog(Format('[%s] %s', [FConfig.Name, Msg]));
end;

function TManagedService.OpenChildLogHandle: THandle;
var
  LogDir, LogPath: string;
  SA: TSecurityAttributes;
begin
  Result := 0;
  try
    LogDir := ExtractFilePath(ParamStr(0)) + 'logs';
    if not DirectoryExists(LogDir) then
      ForceDirectories(LogDir);
    LogPath := IncludeTrailingPathDelimiter(LogDir) + FConfig.Name + '.out.log';

    FillChar(SA, SizeOf(SA), 0);
    SA.nLength := SizeOf(SA);
    SA.bInheritHandle := True;
    SA.lpSecurityDescriptor := nil;

    Result := CreateFile(PChar(LogPath),
      FILE_APPEND_DATA or SYNCHRONIZE,
      FILE_SHARE_READ or FILE_SHARE_WRITE,
      @SA,
      OPEN_ALWAYS,
      FILE_ATTRIBUTE_NORMAL,
      0);

    if Result = INVALID_HANDLE_VALUE then
    begin
      Log(Format('Failed to open child log %s: %s',
        [LogPath, SysErrorMessage(GetLastError)]));
      Result := 0;
    end
    else
      SetFilePointer(Result, 0, nil, FILE_END);
  except
    on E: Exception do
    begin
      Log('OpenChildLogHandle error: ' + E.Message);
      Result := 0;
    end;
  end;
end;

procedure TManagedService.CloseChildHandles;
begin
  if (FChildLogHandle <> 0) and (FChildLogHandle <> INVALID_HANDLE_VALUE) then
  begin
    CloseHandle(FChildLogHandle);
    FChildLogHandle := 0;
  end;
  if FProcessInfo.hProcess <> 0 then
  begin
    CloseHandle(FProcessInfo.hProcess);
    FProcessInfo.hProcess := 0;
  end;
  if FProcessInfo.hThread <> 0 then
  begin
    CloseHandle(FProcessInfo.hThread);
    FProcessInfo.hThread := 0;
  end;
  FProcessInfo.dwProcessId := 0;
  FProcessInfo.dwThreadId := 0;
end;

function TManagedService.StartProcess: Boolean;
var
  StartupInfo: TStartupInfo;
  CommandLine: string;
  WorkDir: PChar;
  CreateFlags: DWORD;
begin
  Result := False;

  CloseChildHandles;
  FChildLogHandle := OpenChildLogHandle;

  ZeroMemory(@StartupInfo, SizeOf(StartupInfo));
  StartupInfo.cb := SizeOf(StartupInfo);
  StartupInfo.dwFlags := STARTF_USESHOWWINDOW;
  StartupInfo.wShowWindow := SW_HIDE;
  CreateFlags := CREATE_NO_WINDOW;

  if (FChildLogHandle <> 0) and (FChildLogHandle <> INVALID_HANDLE_VALUE) then
  begin
    StartupInfo.dwFlags := StartupInfo.dwFlags or STARTF_USESTDHANDLES;
    StartupInfo.hStdInput := 0;
    StartupInfo.hStdOutput := FChildLogHandle;
    StartupInfo.hStdError := FChildLogHandle;
  end;

  CommandLine := Format('"%s" %s', [FConfig.ExecutablePath, FConfig.Arguments]);

  WorkDir := nil;
  if FConfig.WorkingDirectory <> '' then
    WorkDir := PChar(FConfig.WorkingDirectory);

  if CreateProcess(nil, PChar(CommandLine), nil, nil, True,
                   CreateFlags, nil, WorkDir, StartupInfo, FProcessInfo) then
  begin
    FLastStartTime := Now;
    Log(Format('Started pid=%d: %s', [FProcessInfo.dwProcessId, CommandLine]));
    Result := True;
  end
  else
  begin
    Log(Format('CreateProcess failed: %s', [SysErrorMessage(GetLastError)]));
    CloseChildHandles;
  end;
end;

function TManagedService.Start: Boolean;
begin
  Result := False;
  if FStatus = ssRunning then
    Exit(True);

  FStatus := ssStarting;
  FRestartAttempts := 0;
  FStopEvent.ResetEvent;

  try
    if StartProcess then
    begin
      FStatus := ssRunning;
      StartMonitoring;
      Result := True;
    end
    else
      FStatus := ssError;
  except
    on E: Exception do
    begin
      FStatus := ssError;
      Log('Start exception: ' + E.Message);
      CloseChildHandles;
    end;
  end;
end;

function TManagedService.RequestGracefulShutdown(TimeoutMs: DWORD): Boolean;
var
  Info: TEnumCloseInfo;
begin
  Result := False;
  if FProcessInfo.hProcess = 0 then
    Exit(True);

  Info.PID := FProcessInfo.dwProcessId;
  Info.Posted := 0;
  EnumWindows(@EnumCloseProc, LPARAM(@Info));
  if Info.Posted > 0 then
    Log(Format('Posted WM_CLOSE to %d window(s)', [Info.Posted]));

  Result := WaitForSingleObject(FProcessInfo.hProcess, TimeoutMs) = WAIT_OBJECT_0;
end;

procedure TManagedService.Stop;
var
  TimeoutMs: DWORD;
begin
  if FStatus in [ssStopped, ssStopping] then
    Exit;

  FStatus := ssStopping;
  StopMonitoring;

  if FProcessInfo.hProcess <> 0 then
  begin
    TimeoutMs := DWORD(FConfig.StopTimeoutSeconds) * 1000;
    if TimeoutMs = 0 then
      TimeoutMs := 15000;

    if RequestGracefulShutdown(TimeoutMs) then
      Log('Stopped gracefully')
    else
    begin
      Log(Format('Graceful stop timed out after %d ms; terminating', [TimeoutMs]));
      if not TerminateProcess(FProcessInfo.hProcess, 1) then
        Log(Format('TerminateProcess failed: %s', [SysErrorMessage(GetLastError)]));
      WaitForSingleObject(FProcessInfo.hProcess, 5000);
    end;

    CloseChildHandles;
  end;

  FStatus := ssStopped;
end;

procedure TManagedService.Kill;
begin
  FStopEvent.SetEvent;
  if FProcessInfo.hProcess <> 0 then
  begin
    TerminateProcess(FProcessInfo.hProcess, 1);
    WaitForSingleObject(FProcessInfo.hProcess, 5000);
    CloseChildHandles;
  end;
  FStatus := ssStopped;
end;

function TManagedService.IsProcessRunning: Boolean;
begin
  Result := (FProcessInfo.hProcess <> 0) and
            (WaitForSingleObject(FProcessInfo.hProcess, 0) = WAIT_TIMEOUT);
end;

procedure TManagedService.StartMonitoring;
begin
  if FMonitorThread <> nil then
    Exit;

  FStopEvent.ResetEvent;

  FMonitorThread := TThread.CreateAnonymousThread(
    procedure
    var
      WaitHandles: array[0..1] of THandle;
      WaitRet: DWORD;
      Stopped: Boolean;
    begin
      Stopped := False;
      while not Stopped do
      begin
        if FProcessInfo.hProcess = 0 then
          Break;

        WaitHandles[0] := FProcessInfo.hProcess;
        WaitHandles[1] := FStopEvent.Handle;

        WaitRet := WaitForMultipleObjects(2, @WaitHandles[0], False, INFINITE);
        case WaitRet of
          WAIT_OBJECT_0 + 1:
            // Stop requested — caller (Stop) will terminate the child
            Break;

          WAIT_OBJECT_0:
          begin
            // Child exited on its own
            FStatus := ssError;
            Log('Process exited unexpectedly');
            CloseChildHandles;

            if not FConfig.RestartOnFailure or
               (FRestartAttempts >= FConfig.MaxRestartAttempts) then
            begin
              Log('Restart budget exhausted; giving up');
              FStatus := ssStopped;
              Stopped := True;
              Continue;
            end;

            Inc(FRestartAttempts);
            Log(Format('Restart attempt %d/%d in %ds',
              [FRestartAttempts, FConfig.MaxRestartAttempts,
               FConfig.RestartDelaySeconds]));

            // Stop-event-aware backoff so shutdown is not blocked
            if FStopEvent.WaitFor(DWORD(FConfig.RestartDelaySeconds) * 1000)
               = wrSignaled then
            begin
              Stopped := True;
              Continue;
            end;

            if StartProcess then
              FStatus := ssRunning
            else
            begin
              FStatus := ssError;
              Stopped := True;
            end;
          end;
        else
          // WAIT_FAILED or WAIT_TIMEOUT (shouldn't happen with INFINITE)
          Log(Format('Monitor wait failed: ret=%d err=%s',
            [WaitRet, SysErrorMessage(GetLastError)]));
          Stopped := True;
        end;
      end;
    end);

  FMonitorThread.FreeOnTerminate := False;
  FMonitorThread.Start;
end;

procedure TManagedService.StopMonitoring;
var
  T: TThread;
begin
  if FMonitorThread = nil then
    Exit;
  T := FMonitorThread;
  FStopEvent.SetEvent;
  T.WaitFor;
  FMonitorThread := nil;
  T.Free;
end;

{ TServiceOrchestrator }

procedure ServiceController(CtrlCode: DWord); stdcall;
begin
  if Assigned(ServiceOrchestrator) then
    ServiceOrchestrator.Controller(CtrlCode);
end;

constructor TServiceOrchestrator.CreateNew(AOwner: TComponent; Dummy: Integer);
begin
  inherited CreateNew(AOwner, Dummy);

  Name := 'ServiceOrchestrator';
  DisplayName := 'Service Orchestrator';
  AllowPause := True;

  FServices := TObjectDictionary<string, TManagedService>.Create([doOwnsValues]);
  FCriticalSection := TCriticalSection.Create;
  FLogLock := TCriticalSection.Create;
  FShutdownEvent := TEvent.Create(nil, True, False, '');

  FConfigFile := ExtractFilePath(ParamStr(0)) + 'ServiceOrchestrator.ini';
  FLogFile := ExtractFilePath(ParamStr(0)) + 'ServiceOrchestrator.log';

  OnStart := ServiceStart;
  OnStop := ServiceStop;
  OnPause := ServicePause;
  OnContinue := ServiceContinue;
end;

destructor TServiceOrchestrator.Destroy;
begin
  try
    StopAllServices;
  except
    on E: Exception do
      LogMessage('StopAllServices error in destructor: ' + E.Message);
  end;
  FShutdownEvent.Free;
  FLogLock.Free;
  FCriticalSection.Free;
  FServices.Free;
  inherited;
end;

function TServiceOrchestrator.GetServiceController: TServiceController;
begin
  Result := @ServiceController;
end;

procedure TServiceOrchestrator.ServiceStart(Sender: TService; var Started: Boolean);
begin
  try
    LogMessage('Service Orchestrator starting...');
    FShutdownEvent.ResetEvent;
    LoadConfiguration;
    StartAllServices;

    FMonitorThread := TThread.CreateAnonymousThread(
      procedure
      begin
        MonitorServices;
      end);
    FMonitorThread.FreeOnTerminate := False;
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
var
  T: TThread;
begin
  try
    LogMessage('Service Orchestrator stopping...');
    FShutdownEvent.SetEvent;

    T := FMonitorThread;
    FMonitorThread := nil;
    if Assigned(T) then
    try
      T.WaitFor;
    finally
      T.Free;
    end;

    StopAllServices;
    SaveConfiguration;

    Stopped := True;
    LogMessage('Service Orchestrator stopped successfully');
  except
    on E: Exception do
    begin
      LogMessage('Error stopping Service Orchestrator: ' + E.Message);
      Stopped := True;
    end;
  end;
end;

procedure TServiceOrchestrator.ServicePause(Sender: TService; var Paused: Boolean);
var
  Service: TManagedService;
begin
  FCriticalSection.Enter;
  try
    for Service in FServices.Values do
      if Service.Status = ssRunning then
        Service.Stop;
    Paused := True;
  finally
    FCriticalSection.Leave;
  end;
end;

procedure TServiceOrchestrator.ServiceContinue(Sender: TService; var Continued: Boolean);
begin
  StartAllServices;
  Continued := True;
end;

procedure TServiceOrchestrator.LoadConfiguration;
var
  IniFile: TIniFile;
  Sections: TStringList;
  I: Integer;
  ServiceName, DependsOnStr: string;
  Config: TServiceConfig;
  Service: TManagedService;
  Logger: TLogProc;
begin
  if not FileExists(FConfigFile) then
    Exit;

  Logger := procedure(const Msg: string)
            begin
              LogMessage(Msg);
            end;

  IniFile := TIniFile.Create(FConfigFile);
  Sections := TStringList.Create;
  try
    IniFile.ReadSections(Sections);

    for I := 0 to Sections.Count - 1 do
    begin
      ServiceName := Sections[I];
      if SameText(ServiceName, 'GENERAL') then
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
      Config.StopTimeoutSeconds := IniFile.ReadInteger(ServiceName, 'StopTimeoutSeconds', 15);
      Config.Enabled := IniFile.ReadBool(ServiceName, 'Enabled', True);

      DependsOnStr := IniFile.ReadString(ServiceName, 'DependsOn', '');
      if DependsOnStr <> '' then
        Config.DependsOn := SplitString(DependsOnStr, ',;')
      else
        SetLength(Config.DependsOn, 0);

      if Config.ExecutablePath <> '' then
      begin
        Service := TManagedService.Create(Config, Logger);
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
  Sections: TStringList;
  Section, ServiceName: string;
  Service: TManagedService;
begin
  IniFile := TIniFile.Create(FConfigFile);
  try
    Sections := TStringList.Create;
    try
      IniFile.ReadSections(Sections);
      for Section in Sections do
        if not SameText(Section, 'GENERAL') then
          IniFile.EraseSection(Section);
    finally
      Sections.Free;
    end;

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
      IniFile.WriteInteger(ServiceName, 'StopTimeoutSeconds', Service.Config.StopTimeoutSeconds);
      IniFile.WriteBool(ServiceName, 'Enabled', Service.Config.Enabled);
      if Length(Service.Config.DependsOn) > 0 then
        IniFile.WriteString(ServiceName, 'DependsOn',
          string.Join(',', Service.Config.DependsOn))
      else
        IniFile.DeleteKey(ServiceName, 'DependsOn');
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
  try
    StartOrder := ResolveDependencies;
  except
    on E: Exception do
    begin
      LogMessage('Dependency resolution failed: ' + E.Message);
      Exit;
    end;
  end;

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
        try
          Service.Stop;
        except
          on E: Exception do
            LogMessage(Format('Error stopping %s: %s', [ServiceName, E.Message]));
        end;
      end;
    end;
  finally
    FCriticalSection.Leave;
  end;
end;

procedure TServiceOrchestrator.MonitorServices;
var
  ServiceName: string;
  Service: TManagedService;
begin
  // Per-service monitor threads own restart logic.
  // This loop only observes and logs aggregate health.
  while FShutdownEvent.WaitFor(5000) <> wrSignaled do
  begin
    FCriticalSection.Enter;
    try
      for ServiceName in FServices.Keys do
      begin
        Service := FServices[ServiceName];
        if Service.Config.Enabled and (Service.Status = ssError) then
          LogMessage(Format('Service %s is in error state', [ServiceName]));
      end;
    finally
      FCriticalSection.Leave;
    end;
  end;
end;

procedure TServiceOrchestrator.LogMessage(const Msg: string);
var
  LogEntry: AnsiString;
  Stream: TFileStream;
begin
  LogEntry := AnsiString(Format('[%s] %s'#13#10,
    [FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now), Msg]));

  FLogLock.Enter;
  try
    try
      if FileExists(FLogFile) then
        Stream := TFileStream.Create(FLogFile, fmOpenWrite or fmShareDenyWrite)
      else
        Stream := TFileStream.Create(FLogFile, fmCreate or fmShareDenyWrite);
      try
        Stream.Seek(0, soEnd);
        if Length(LogEntry) > 0 then
          Stream.WriteBuffer(LogEntry[1], Length(LogEntry));
      finally
        Stream.Free;
      end;
    except
      // Last-resort: swallow — never let logging crash the service
    end;
  finally
    FLogLock.Leave;
  end;
end;

function TServiceOrchestrator.ResolveDependencies: TArray<string>;
type
  TVisitState = (vsUnvisited, vsVisiting, vsDone);
var
  ResultList: TList<string>;
  State: TDictionary<string, TVisitState>;

  procedure Visit(const ServiceName: string; const Path: TStringList);
  var
    Service: TManagedService;
    Dependency: string;
    Current: TVisitState;
  begin
    if State.TryGetValue(ServiceName, Current) then
    begin
      if Current = vsDone then
        Exit;
      if Current = vsVisiting then
      begin
        Path.Add(ServiceName);
        raise Exception.CreateFmt(
          'Dependency cycle detected: %s',
          [StringReplace(Path.CommaText, ',', ' -> ', [rfReplaceAll])]);
      end;
    end;

    if not FServices.TryGetValue(ServiceName, Service) then
    begin
      // Dependency refers to an unknown service — record and skip
      State.AddOrSetValue(ServiceName, vsDone);
      Exit;
    end;

    State.AddOrSetValue(ServiceName, vsVisiting);
    Path.Add(ServiceName);
    try
      for Dependency in Service.Config.DependsOn do
        Visit(Dependency, Path);
    finally
      Path.Delete(Path.Count - 1);
    end;
    State.AddOrSetValue(ServiceName, vsDone);
    ResultList.Add(ServiceName);
  end;

var
  ServiceName: string;
  Path: TStringList;
begin
  ResultList := TList<string>.Create;
  State := TDictionary<string, TVisitState>.Create;
  Path := TStringList.Create;
  try
    for ServiceName in FServices.Keys do
      Visit(ServiceName, Path);
    Result := ResultList.ToArray;
  finally
    Path.Free;
    State.Free;
    ResultList.Free;
  end;
end;

function TServiceOrchestrator.AddService(const AName, ADisplayName, AExecutablePath: string;
  const AArguments, AWorkingDir: string): Boolean;
var
  Config: TServiceConfig;
  Service: TManagedService;
  Logger: TLogProc;
begin
  Result := False;

  FCriticalSection.Enter;
  try
    if FServices.ContainsKey(AName) then
      Exit;

    Config := TServiceConfig.Create;
    Config.Name := AName;
    Config.DisplayName := ADisplayName;
    Config.ExecutablePath := AExecutablePath;
    Config.Arguments := AArguments;
    Config.WorkingDirectory := AWorkingDir;

    Logger := procedure(const Msg: string)
              begin
                LogMessage(Msg);
              end;

    Service := TManagedService.Create(Config, Logger);
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

begin
  if not Application.DelayInitialize or Application.Installing then
    Application.Initialize;

  ServiceOrchestrator := TServiceOrchestrator.CreateNew(Application);
  Application.Run;
end.

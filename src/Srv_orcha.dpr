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
  System.JSON,
  Vcl.SvcMgr,
  OrchaLogging in 'OrchaLogging.pas',
  OrchaMetrics in 'OrchaMetrics.pas',
  OrchaHealth in 'OrchaHealth.pas',
  OrchaHttp in 'OrchaHttp.pas';

{$R *.RES}

type
  TServiceStatus = (ssStarting, ssRunning, ssStopping, ssStopped, ssError);

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
    HealthCheck: THealthCheckConfig;
    constructor Create;
  end;

  TManagedService = class
  private
    FConfig: TServiceConfig;
    FProcessInfo: TProcessInformation;
    FStatus: TServiceStatus;
    FHealthState: THealthState;
    FConsecutiveHealthFailures: Integer;
    FRestartAttempts: Integer;
    FLastStartTime: TDateTime;
    FMonitorThread: TThread;
    FStopEvent: TEvent;
    FChildLogHandle: THandle;

    FLogger:   TStructuredLogger;
    FMetrics:  TMetricsRegistry;
    FEventLog: TEventLogWriter;

    function StartProcess(const ATraceID: string): Boolean;
    procedure StartMonitoring;
    procedure StopMonitoring;
    function IsProcessRunning: Boolean;
    procedure LogEvent(ALevel: TLogLevel; const AEvent, AMessage: string;
      const ATraceID: string = ''); overload;
    procedure LogEvent(ALevel: TLogLevel; const AEvent, AMessage, ATraceID: string;
      const AFields: array of TLogField); overload;
    function OpenChildLogHandle: THandle;
    procedure CloseChildHandles;
    function RequestGracefulShutdown(TimeoutMs: DWORD): Boolean;
    procedure HandleHealthProbe;
    procedure TransitionHealth(ANewState: THealthState; const AReason, ATraceID: string);
  public
    constructor Create(AConfig: TServiceConfig;
      ALogger: TStructuredLogger;
      AMetrics: TMetricsRegistry;
      AEventLog: TEventLogWriter);
    destructor Destroy; override;

    function Start: Boolean;
    procedure Stop;
    procedure Kill;

    property Config: TServiceConfig read FConfig;
    property Status: TServiceStatus read FStatus;
    property HealthState: THealthState read FHealthState;
    property ProcessId: DWORD read FProcessInfo.dwProcessId;
  end;

  TServiceOrchestrator = class(TService)
  private
    FServices: TObjectDictionary<string, TManagedService>;
    FConfigFile: string;
    FCriticalSection: TCriticalSection;
    FShutdownEvent: TEvent;
    FMonitorThread: TThread;

    // Observability
    FLogger: TStructuredLogger;
    FEventLog: TEventLogWriter;
    FMetrics: TMetricsRegistry;
    FMetricsServer: TMetricsServer;

    // General settings cache
    FMetricsEnabled: Boolean;
    FMetricsPort: Integer;
    FMetricsBind: string;
    FLogPath: string;
    FLogLevel: TLogLevel;
    FLogMaxFileSizeBytes: Int64;
    FLogMaxFiles: Integer;
    FEventLogEnabled: Boolean;
    FEventLogSource: string;

    procedure LoadGeneralSettings;
    procedure LoadConfiguration;
    procedure SaveConfiguration;
    procedure StartAllServices(const ATraceID: string);
    procedure StopAllServices(const ATraceID: string);
    procedure MonitorServices;
    procedure LogMessage(const Msg: string);
    function ResolveDependencies: TArray<string>;

    // HTTP callbacks
    function HttpRenderMetrics: string;
    function HttpRenderServices: string;
    function HttpRenderHealth: string;
    function HttpCheckReadiness(out AJson: string): Boolean;
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

    property Logger: TStructuredLogger read FLogger;
    property Metrics: TMetricsRegistry read FMetrics;
    property EventLog: TEventLogWriter read FEventLog;
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
  HealthCheck := THealthCheckConfig.Defaults;
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

constructor TManagedService.Create(AConfig: TServiceConfig;
  ALogger: TStructuredLogger;
  AMetrics: TMetricsRegistry;
  AEventLog: TEventLogWriter);
begin
  inherited Create;
  FConfig := AConfig;
  FLogger := ALogger;
  FMetrics := AMetrics;
  FEventLog := AEventLog;
  FStatus := ssStopped;
  FHealthState := hsUnknown;
  FConsecutiveHealthFailures := 0;
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
      LogEvent(llError, 'destroy_error', E.Message);
  end;
  CloseChildHandles;
  FStopEvent.Free;
  FConfig.Free;
  inherited;
end;

procedure TManagedService.LogEvent(ALevel: TLogLevel;
  const AEvent, AMessage, ATraceID: string;
  const AFields: array of TLogField);
begin
  if Assigned(FLogger) then
    FLogger.Log(ALevel, FConfig.Name, AEvent, AMessage, ATraceID, AFields);
end;

procedure TManagedService.LogEvent(ALevel: TLogLevel;
  const AEvent, AMessage: string; const ATraceID: string);
var
  Empty: array of TLogField;
begin
  SetLength(Empty, 0);
  LogEvent(ALevel, AEvent, AMessage, ATraceID, Empty);
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
      LogEvent(llWarn, 'child_log_open_failed',
        Format('%s: %s', [LogPath, SysErrorMessage(GetLastError)]));
      Result := 0;
    end
    else
      SetFilePointer(Result, 0, nil, FILE_END);
  except
    on E: Exception do
    begin
      LogEvent(llError, 'child_log_open_exception', E.Message);
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

function TManagedService.StartProcess(const ATraceID: string): Boolean;
var
  StartupInfo: TStartupInfo;
  CommandLine: string;
  WorkDir: PChar;
  CreateFlags: DWORD;
  Fields: array of TLogField;
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
    SetLength(Fields, 2);
    Fields[0] := MakeField('pid', IntToStr(FProcessInfo.dwProcessId));
    Fields[1] := MakeField('cmd', CommandLine);
    LogEvent(llInfo, 'child_started', 'process spawned', ATraceID, Fields);

    if Assigned(FMetrics) then
      FMetrics.SetPid(FConfig.Name, FProcessInfo.dwProcessId);

    if Assigned(FEventLog) then
      FEventLog.Info(Format('[%s] started pid=%d',
        [FConfig.Name, FProcessInfo.dwProcessId]));

    Result := True;
  end
  else
  begin
    LogEvent(llError, 'create_process_failed',
      SysErrorMessage(GetLastError), ATraceID);
    if Assigned(FEventLog) then
      FEventLog.Error(Format('[%s] CreateProcess failed: %s',
        [FConfig.Name, SysErrorMessage(GetLastError)]));
    CloseChildHandles;
  end;
end;

function TManagedService.Start: Boolean;
var
  TraceID: string;
begin
  Result := False;
  if FStatus = ssRunning then
    Exit(True);

  TraceID := NewTraceID;
  FStatus := ssStarting;
  FRestartAttempts := 0;
  FConsecutiveHealthFailures := 0;
  FStopEvent.ResetEvent;
  TransitionHealth(hsStarting, 'start_requested', TraceID);

  try
    if StartProcess(TraceID) then
    begin
      FStatus := ssRunning;
      if Assigned(FMetrics) then
        FMetrics.RecordStart(FConfig.Name);
      StartMonitoring;
      Result := True;
    end
    else
    begin
      FStatus := ssError;
      if Assigned(FMetrics) then
        FMetrics.SetUp(FConfig.Name, False);
    end;
  except
    on E: Exception do
    begin
      FStatus := ssError;
      LogEvent(llError, 'start_exception', E.Message, TraceID);
      CloseChildHandles;
      if Assigned(FMetrics) then
        FMetrics.SetUp(FConfig.Name, False);
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

  Result := WaitForSingleObject(FProcessInfo.hProcess, TimeoutMs) = WAIT_OBJECT_0;
end;

procedure TManagedService.Stop;
var
  TimeoutMs: DWORD;
  TraceID: string;
begin
  if FStatus in [ssStopped, ssStopping] then
    Exit;

  TraceID := NewTraceID;
  FStatus := ssStopping;
  LogEvent(llInfo, 'stop_requested', '', TraceID);

  StopMonitoring;

  if FProcessInfo.hProcess <> 0 then
  begin
    TimeoutMs := DWORD(FConfig.StopTimeoutSeconds) * 1000;
    if TimeoutMs = 0 then
      TimeoutMs := 15000;

    if RequestGracefulShutdown(TimeoutMs) then
      LogEvent(llInfo, 'stopped_gracefully', '', TraceID)
    else
    begin
      LogEvent(llWarn, 'stop_timed_out',
        Format('forcing terminate after %d ms', [TimeoutMs]), TraceID);
      if not TerminateProcess(FProcessInfo.hProcess, 1) then
        LogEvent(llError, 'terminate_process_failed',
          SysErrorMessage(GetLastError), TraceID);
      WaitForSingleObject(FProcessInfo.hProcess, 5000);
    end;

    CloseChildHandles;
  end;

  if Assigned(FMetrics) then
  begin
    FMetrics.SetUp(FConfig.Name, False);
    FMetrics.SetHealthy(FConfig.Name, False);
    FMetrics.SetPid(FConfig.Name, 0);
  end;
  TransitionHealth(hsUnknown, 'stopped', TraceID);

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
  if Assigned(FMetrics) then
    FMetrics.SetUp(FConfig.Name, False);
end;

function TManagedService.IsProcessRunning: Boolean;
begin
  Result := (FProcessInfo.hProcess <> 0) and
            (WaitForSingleObject(FProcessInfo.hProcess, 0) = WAIT_TIMEOUT);
end;

procedure TManagedService.TransitionHealth(ANewState: THealthState;
  const AReason, ATraceID: string);
var
  Fields: array of TLogField;
begin
  if FHealthState = ANewState then
    Exit;

  SetLength(Fields, 2);
  Fields[0] := MakeField('from', HealthStateName(FHealthState));
  Fields[1] := MakeField('to', HealthStateName(ANewState));
  LogEvent(llInfo, 'health_transition', AReason, ATraceID, Fields);

  FHealthState := ANewState;

  if Assigned(FMetrics) then
    FMetrics.SetHealthy(FConfig.Name, ANewState = hsHealthy);

  if Assigned(FEventLog) then
    case ANewState of
      hsHealthy:
        FEventLog.Info(Format('[%s] healthy: %s', [FConfig.Name, AReason]));
      hsUnhealthy:
        FEventLog.Warn(Format('[%s] unhealthy: %s', [FConfig.Name, AReason]));
    end;
end;

procedure TManagedService.HandleHealthProbe;
var
  Ok: Boolean;
  ErrMsg, TraceID: string;
  GraceMs: Int64;
  Fields: array of TLogField;
begin
  if FConfig.HealthCheck.Kind = hckNone then
  begin
    // No probe configured: treat as healthy once past startup grace
    GraceMs := Int64(FConfig.HealthCheck.StartupGraceSeconds) * 1000;
    if MilliSecondsBetween(Now, FLastStartTime) >= GraceMs then
    begin
      if FHealthState <> hsHealthy then
        TransitionHealth(hsHealthy, 'no_probe_grace_elapsed', NewTraceID);
    end;
    Exit;
  end;

  // Respect startup grace
  GraceMs := Int64(FConfig.HealthCheck.StartupGraceSeconds) * 1000;
  if MilliSecondsBetween(Now, FLastStartTime) < GraceMs then
    Exit;

  Ok := RunHealthProbe(FConfig.HealthCheck, ErrMsg);

  if Ok then
  begin
    FConsecutiveHealthFailures := 0;
    if FHealthState <> hsHealthy then
      TransitionHealth(hsHealthy, 'probe_succeeded', NewTraceID);
    Exit;
  end;

  // Probe failed
  Inc(FConsecutiveHealthFailures);
  if Assigned(FMetrics) then
    FMetrics.RecordHealthFailure(FConfig.Name);

  SetLength(Fields, 3);
  Fields[0] := MakeField('kind', HealthKindName(FConfig.HealthCheck.Kind));
  Fields[1] := MakeField('target', FConfig.HealthCheck.Target);
  Fields[2] := MakeField('consecutive_failures',
    IntToStr(FConsecutiveHealthFailures));
  LogEvent(llWarn, 'health_probe_failed', ErrMsg, '', Fields);

  if FConsecutiveHealthFailures >= FConfig.HealthCheck.FailureThreshold then
  begin
    if FHealthState <> hsUnhealthy then
    begin
      TraceID := NewTraceID;
      TransitionHealth(hsUnhealthy,
        Format('%d consecutive probe failures', [FConsecutiveHealthFailures]),
        TraceID);

      if FConfig.HealthCheck.RestartOnUnhealthy and (FProcessInfo.hProcess <> 0) then
      begin
        LogEvent(llWarn, 'kill_unhealthy',
          'terminating child; crash-restart path will respawn it', TraceID);
        TerminateProcess(FProcessInfo.hProcess, 1);
        // The WAIT_OBJECT_0 branch in the monitor loop will handle restart.
      end;
    end;
  end;
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
      WaitRet, WaitMs: DWORD;
      Stopped: Boolean;
      TraceID: string;
    begin
      Stopped := False;
      while not Stopped do
      begin
        if FProcessInfo.hProcess = 0 then
          Break;

        WaitHandles[0] := FProcessInfo.hProcess;
        WaitHandles[1] := FStopEvent.Handle;

        if FConfig.HealthCheck.Kind <> hckNone then
          WaitMs := DWORD(FConfig.HealthCheck.IntervalSeconds) * 1000
        else if FConfig.HealthCheck.StartupGraceSeconds > 0 then
          // Even with no probe, wake periodically to transition state
          WaitMs := DWORD(FConfig.HealthCheck.StartupGraceSeconds) * 1000
        else
          WaitMs := INFINITE;

        if WaitMs = 0 then
          WaitMs := 30000;

        WaitRet := WaitForMultipleObjects(2, @WaitHandles[0], False, WaitMs);

        case WaitRet of
          WAIT_OBJECT_0 + 1:
            Break;

          WAIT_TIMEOUT:
          begin
            try
              HandleHealthProbe;
            except
              on E: Exception do
                LogEvent(llError, 'health_probe_exception', E.Message);
            end;
          end;

          WAIT_OBJECT_0:
          begin
            TraceID := NewTraceID;
            FStatus := ssError;
            LogEvent(llWarn, 'child_exited_unexpectedly', '', TraceID);
            CloseChildHandles;
            if Assigned(FMetrics) then
              FMetrics.RecordCrash(FConfig.Name);
            if Assigned(FEventLog) then
              FEventLog.Warn(Format('[%s] process exited unexpectedly',
                [FConfig.Name]));

            TransitionHealth(hsUnhealthy, 'process_exited', TraceID);

            if not FConfig.RestartOnFailure or
               (FRestartAttempts >= FConfig.MaxRestartAttempts) then
            begin
              LogEvent(llError, 'restart_budget_exhausted', '', TraceID);
              if Assigned(FEventLog) then
                FEventLog.Error(Format('[%s] restart budget exhausted',
                  [FConfig.Name]));
              FStatus := ssStopped;
              Stopped := True;
              Continue;
            end;

            Inc(FRestartAttempts);
            LogEvent(llInfo, 'restart_scheduled',
              Format('attempt %d/%d in %ds',
                [FRestartAttempts, FConfig.MaxRestartAttempts,
                 FConfig.RestartDelaySeconds]), TraceID);

            if FStopEvent.WaitFor(
                 DWORD(FConfig.RestartDelaySeconds) * 1000) = wrSignaled then
            begin
              Stopped := True;
              Continue;
            end;

            if StartProcess(TraceID) then
            begin
              FStatus := ssRunning;
              FConsecutiveHealthFailures := 0;
              if Assigned(FMetrics) then
                FMetrics.RecordRestart(FConfig.Name);
              TransitionHealth(hsStarting, 'restarted', TraceID);
            end
            else
            begin
              FStatus := ssError;
              Stopped := True;
            end;
          end;
        else
          LogEvent(llError, 'monitor_wait_failed',
            Format('ret=%d err=%s',
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
  FShutdownEvent := TEvent.Create(nil, True, False, '');

  FConfigFile := ExtractFilePath(ParamStr(0)) + 'ServiceOrchestrator.ini';

  // Defaults (overridden by INI)
  FMetricsEnabled := True;
  FMetricsPort := 9464;
  FMetricsBind := '0.0.0.0';
  FLogPath := ExtractFilePath(ParamStr(0)) + 'logs\orcha.jsonl';
  FLogLevel := llInfo;
  FLogMaxFileSizeBytes := 10 * 1024 * 1024;
  FLogMaxFiles := 7;
  FEventLogEnabled := True;
  FEventLogSource := 'ServiceOrchestrator';

  OnStart := ServiceStart;
  OnStop := ServiceStop;
  OnPause := ServicePause;
  OnContinue := ServiceContinue;
end;

destructor TServiceOrchestrator.Destroy;
begin
  try
    if Assigned(FMetricsServer) then
      FMetricsServer.Stop;
  except
    // ignore
  end;

  try
    StopAllServices(NewTraceID);
  except
    on E: Exception do
      LogMessage('StopAllServices error in destructor: ' + E.Message);
  end;

  FreeAndNil(FMetricsServer);
  FreeAndNil(FServices);
  FreeAndNil(FMetrics);
  FreeAndNil(FEventLog);
  FreeAndNil(FShutdownEvent);
  FreeAndNil(FCriticalSection);
  FreeAndNil(FLogger);
  inherited;
end;

function TServiceOrchestrator.GetServiceController: TServiceController;
begin
  Result := @ServiceController;
end;

procedure TServiceOrchestrator.LoadGeneralSettings;
var
  Ini: TIniFile;
begin
  if not FileExists(FConfigFile) then
    Exit;
  Ini := TIniFile.Create(FConfigFile);
  try
    FMetricsEnabled      := Ini.ReadBool   ('GENERAL', 'MetricsEnabled',      FMetricsEnabled);
    FMetricsPort         := Ini.ReadInteger('GENERAL', 'MetricsPort',         FMetricsPort);
    FMetricsBind         := Ini.ReadString ('GENERAL', 'MetricsBind',         FMetricsBind);
    FLogPath             := Ini.ReadString ('GENERAL', 'LogPath',             FLogPath);
    FLogLevel            := ParseLogLevel(Ini.ReadString('GENERAL', 'LogLevel', 'info'), FLogLevel);
    FLogMaxFileSizeBytes := Ini.ReadInteger('GENERAL', 'LogMaxFileSizeBytes', FLogMaxFileSizeBytes);
    FLogMaxFiles         := Ini.ReadInteger('GENERAL', 'LogMaxFiles',         FLogMaxFiles);
    FEventLogEnabled     := Ini.ReadBool   ('GENERAL', 'EventLogEnabled',     FEventLogEnabled);
    FEventLogSource      := Ini.ReadString ('GENERAL', 'EventLogSource',      FEventLogSource);
  finally
    Ini.Free;
  end;
end;

procedure TServiceOrchestrator.ServiceStart(Sender: TService; var Started: Boolean);
var
  StartTrace: string;
begin
  Started := False;
  try
    LoadGeneralSettings;

    // Observability stack
    FLogger := TStructuredLogger.Create(FLogPath, FLogLevel,
      FLogMaxFileSizeBytes, FLogMaxFiles);
    FEventLog := TEventLogWriter.Create(FEventLogSource, FEventLogEnabled);
    FMetrics := TMetricsRegistry.Create;

    StartTrace := NewTraceID;
    FLogger.Log(llInfo, '', 'orchestrator_starting',
      'Service Orchestrator starting', StartTrace, []);
    FEventLog.Info('Service Orchestrator starting');

    FShutdownEvent.ResetEvent;
    LoadConfiguration;
    StartAllServices(StartTrace);

    if FMetricsEnabled then
    begin
      FMetricsServer := TMetricsServer.Create(FMetricsPort, FMetricsBind,
        HttpRenderMetrics, HttpRenderServices, HttpRenderHealth,
        HttpCheckReadiness);
      try
        FMetricsServer.Start;
        FLogger.Log(llInfo, '', 'metrics_server_started',
          Format('listening on %s:%d', [FMetricsBind, FMetricsPort]));
      except
        on E: Exception do
        begin
          FLogger.Log(llError, '', 'metrics_server_failed',
            E.Message, StartTrace, []);
          FEventLog.Error('Metrics server failed: ' + E.Message);
          FreeAndNil(FMetricsServer);
        end;
      end;
    end;

    FMonitorThread := TThread.CreateAnonymousThread(
      procedure
      begin
        MonitorServices;
      end);
    FMonitorThread.FreeOnTerminate := False;
    FMonitorThread.Start;

    Started := True;
    FLogger.Log(llInfo, '', 'orchestrator_started',
      'Service Orchestrator started', StartTrace, []);
    FEventLog.Info('Service Orchestrator started');
  except
    on E: Exception do
    begin
      LogMessage('Failed to start Service Orchestrator: ' + E.Message);
      if Assigned(FEventLog) then
        FEventLog.Error('Failed to start: ' + E.Message);
      Started := False;
    end;
  end;
end;

procedure TServiceOrchestrator.ServiceStop(Sender: TService; var Stopped: Boolean);
var
  T: TThread;
  StopTrace: string;
begin
  try
    StopTrace := NewTraceID;
    LogMessage('Service Orchestrator stopping...');
    if Assigned(FEventLog) then
      FEventLog.Info('Service Orchestrator stopping');

    FShutdownEvent.SetEvent;

    if Assigned(FMetricsServer) then
    try
      FMetricsServer.Stop;
    except
      // ignore
    end;

    T := FMonitorThread;
    FMonitorThread := nil;
    if Assigned(T) then
    try
      T.WaitFor;
    finally
      T.Free;
    end;

    StopAllServices(StopTrace);
    SaveConfiguration;

    Stopped := True;
    LogMessage('Service Orchestrator stopped successfully');
    if Assigned(FEventLog) then
      FEventLog.Info('Service Orchestrator stopped');
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
  StartAllServices(NewTraceID);
  Continued := True;
end;

procedure TServiceOrchestrator.LoadConfiguration;
var
  Ini: TIniFile;
  Sections: TStringList;
  I: Integer;
  ServiceName, DependsOnStr: string;
  Config: TServiceConfig;
  Service: TManagedService;
begin
  if not FileExists(FConfigFile) then
    Exit;

  Ini := TIniFile.Create(FConfigFile);
  Sections := TStringList.Create;
  try
    Ini.ReadSections(Sections);

    for I := 0 to Sections.Count - 1 do
    begin
      ServiceName := Sections[I];
      if SameText(ServiceName, 'GENERAL') then
        Continue;

      Config := TServiceConfig.Create;
      Config.Name := ServiceName;
      Config.DisplayName := Ini.ReadString(ServiceName, 'DisplayName', ServiceName);
      Config.ExecutablePath := Ini.ReadString(ServiceName, 'ExecutablePath', '');
      Config.Arguments := Ini.ReadString(ServiceName, 'Arguments', '');
      Config.WorkingDirectory := Ini.ReadString(ServiceName, 'WorkingDirectory', '');
      Config.RestartOnFailure := Ini.ReadBool(ServiceName, 'RestartOnFailure', True);
      Config.MaxRestartAttempts := Ini.ReadInteger(ServiceName, 'MaxRestartAttempts', 3);
      Config.RestartDelaySeconds := Ini.ReadInteger(ServiceName, 'RestartDelaySeconds', 10);
      Config.StopTimeoutSeconds := Ini.ReadInteger(ServiceName, 'StopTimeoutSeconds', 15);
      Config.Enabled := Ini.ReadBool(ServiceName, 'Enabled', True);

      DependsOnStr := Ini.ReadString(ServiceName, 'DependsOn', '');
      if DependsOnStr <> '' then
        Config.DependsOn := SplitString(DependsOnStr, ',;')
      else
        SetLength(Config.DependsOn, 0);

      Config.HealthCheck.Kind :=
        ParseHealthKind(Ini.ReadString(ServiceName, 'HealthCheckKind', 'none'));
      Config.HealthCheck.Target :=
        Ini.ReadString(ServiceName, 'HealthCheckTarget', '');
      Config.HealthCheck.IntervalSeconds :=
        Ini.ReadInteger(ServiceName, 'HealthCheckIntervalSeconds', 30);
      Config.HealthCheck.TimeoutMs :=
        Ini.ReadInteger(ServiceName, 'HealthCheckTimeoutMs', 3000);
      Config.HealthCheck.FailureThreshold :=
        Ini.ReadInteger(ServiceName, 'HealthCheckFailureThreshold', 3);
      Config.HealthCheck.StartupGraceSeconds :=
        Ini.ReadInteger(ServiceName, 'HealthCheckStartupGraceSeconds', 10);
      Config.HealthCheck.RestartOnUnhealthy :=
        Ini.ReadBool(ServiceName, 'RestartOnUnhealthy', False);

      if Config.ExecutablePath <> '' then
      begin
        Service := TManagedService.Create(Config, FLogger, FMetrics, FEventLog);
        FServices.Add(ServiceName, Service);
      end
      else
        Config.Free;
    end;
  finally
    Sections.Free;
    Ini.Free;
  end;
end;

procedure TServiceOrchestrator.SaveConfiguration;
var
  Ini: TIniFile;
  Sections: TStringList;
  Section, ServiceName: string;
  Service: TManagedService;
begin
  Ini := TIniFile.Create(FConfigFile);
  try
    Sections := TStringList.Create;
    try
      Ini.ReadSections(Sections);
      for Section in Sections do
        if not SameText(Section, 'GENERAL') then
          Ini.EraseSection(Section);
    finally
      Sections.Free;
    end;

    for ServiceName in FServices.Keys do
    begin
      Service := FServices[ServiceName];
      Ini.WriteString (ServiceName, 'DisplayName',         Service.Config.DisplayName);
      Ini.WriteString (ServiceName, 'ExecutablePath',      Service.Config.ExecutablePath);
      Ini.WriteString (ServiceName, 'Arguments',           Service.Config.Arguments);
      Ini.WriteString (ServiceName, 'WorkingDirectory',    Service.Config.WorkingDirectory);
      Ini.WriteBool   (ServiceName, 'RestartOnFailure',    Service.Config.RestartOnFailure);
      Ini.WriteInteger(ServiceName, 'MaxRestartAttempts',  Service.Config.MaxRestartAttempts);
      Ini.WriteInteger(ServiceName, 'RestartDelaySeconds', Service.Config.RestartDelaySeconds);
      Ini.WriteInteger(ServiceName, 'StopTimeoutSeconds',  Service.Config.StopTimeoutSeconds);
      Ini.WriteBool   (ServiceName, 'Enabled',             Service.Config.Enabled);

      if Length(Service.Config.DependsOn) > 0 then
        Ini.WriteString(ServiceName, 'DependsOn',
          string.Join(',', Service.Config.DependsOn))
      else
        Ini.DeleteKey(ServiceName, 'DependsOn');

      Ini.WriteString (ServiceName, 'HealthCheckKind',
        HealthKindName(Service.Config.HealthCheck.Kind));
      Ini.WriteString (ServiceName, 'HealthCheckTarget',
        Service.Config.HealthCheck.Target);
      Ini.WriteInteger(ServiceName, 'HealthCheckIntervalSeconds',
        Service.Config.HealthCheck.IntervalSeconds);
      Ini.WriteInteger(ServiceName, 'HealthCheckTimeoutMs',
        Service.Config.HealthCheck.TimeoutMs);
      Ini.WriteInteger(ServiceName, 'HealthCheckFailureThreshold',
        Service.Config.HealthCheck.FailureThreshold);
      Ini.WriteInteger(ServiceName, 'HealthCheckStartupGraceSeconds',
        Service.Config.HealthCheck.StartupGraceSeconds);
      Ini.WriteBool   (ServiceName, 'RestartOnUnhealthy',
        Service.Config.HealthCheck.RestartOnUnhealthy);
    end;
  finally
    Ini.Free;
  end;
end;

procedure TServiceOrchestrator.StartAllServices(const ATraceID: string);
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
      if Assigned(FLogger) then
        FLogger.Log(llError, '', 'dependency_resolution_failed',
          E.Message, ATraceID, []);
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
          if Assigned(FLogger) then
            FLogger.Log(llInfo, ServiceName, 'starting', '', ATraceID, []);
          Service.Start;
        except
          on E: Exception do
            if Assigned(FLogger) then
              FLogger.Log(llError, ServiceName, 'start_failed',
                E.Message, ATraceID, []);
        end;
      end;
    end;
  finally
    FCriticalSection.Leave;
  end;
end;

procedure TServiceOrchestrator.StopAllServices(const ATraceID: string);
var
  ServiceName: string;
  Service: TManagedService;
begin
  if not Assigned(FServices) then
    Exit;

  FCriticalSection.Enter;
  try
    for ServiceName in FServices.Keys do
    begin
      Service := FServices[ServiceName];
      if Service.Status = ssRunning then
      begin
        if Assigned(FLogger) then
          FLogger.Log(llInfo, ServiceName, 'stopping', '', ATraceID, []);
        try
          Service.Stop;
        except
          on E: Exception do
            if Assigned(FLogger) then
              FLogger.Log(llError, ServiceName, 'stop_failed',
                E.Message, ATraceID, []);
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
  while FShutdownEvent.WaitFor(5000) <> wrSignaled do
  begin
    FCriticalSection.Enter;
    try
      for ServiceName in FServices.Keys do
      begin
        Service := FServices[ServiceName];
        if Service.Config.Enabled and (Service.Status = ssError) and
           Assigned(FLogger) then
          FLogger.Log(llWarn, ServiceName, 'error_state',
            'service remains in error state');
      end;
    finally
      FCriticalSection.Leave;
    end;
  end;
end;

procedure TServiceOrchestrator.LogMessage(const Msg: string);
begin
  if Assigned(FLogger) then
    FLogger.Log(llInfo, '', 'message', Msg);
end;

function TServiceOrchestrator.ResolveDependencies: TArray<string>;
type
  TVisitState = (vsUnvisited, vsVisiting, vsDone);
var
  ResultList: TList<string>;
  State: TDictionary<string, TVisitState>;
  Path: TStringList;

  procedure Visit(const ServiceName: string);
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
        raise Exception.CreateFmt('Dependency cycle: %s',
          [StringReplace(Path.CommaText, ',', ' -> ', [rfReplaceAll])]);
      end;
    end;

    if not FServices.TryGetValue(ServiceName, Service) then
    begin
      State.AddOrSetValue(ServiceName, vsDone);
      Exit;
    end;

    State.AddOrSetValue(ServiceName, vsVisiting);
    Path.Add(ServiceName);
    try
      for Dependency in Service.Config.DependsOn do
        Visit(Dependency);
    finally
      Path.Delete(Path.Count - 1);
    end;
    State.AddOrSetValue(ServiceName, vsDone);
    ResultList.Add(ServiceName);
  end;

var
  ServiceName: string;
begin
  ResultList := TList<string>.Create;
  State := TDictionary<string, TVisitState>.Create;
  Path := TStringList.Create;
  try
    for ServiceName in FServices.Keys do
      Visit(ServiceName);
    Result := ResultList.ToArray;
  finally
    Path.Free;
    State.Free;
    ResultList.Free;
  end;
end;

{ HTTP callbacks }

function TServiceOrchestrator.HttpRenderMetrics: string;
begin
  if Assigned(FMetrics) then
    Result := FMetrics.Render
  else
    Result := '';
end;

function StatusName(AStatus: TServiceStatus): string;
begin
  case AStatus of
    ssStarting: Result := 'starting';
    ssRunning:  Result := 'running';
    ssStopping: Result := 'stopping';
    ssStopped:  Result := 'stopped';
    ssError:    Result := 'error';
  else
    Result := 'unknown';
  end;
end;

function TServiceOrchestrator.HttpRenderServices: string;
var
  Arr: TJSONArray;
  Obj: TJSONObject;
  Service: TManagedService;
  ServiceName: string;
begin
  Arr := TJSONArray.Create;
  try
    FCriticalSection.Enter;
    try
      for ServiceName in FServices.Keys do
      begin
        Service := FServices[ServiceName];
        Obj := TJSONObject.Create;
        Obj.AddPair('name', ServiceName);
        Obj.AddPair('display_name', Service.Config.DisplayName);
        Obj.AddPair('enabled', TJSONBool.Create(Service.Config.Enabled));
        Obj.AddPair('status', StatusName(Service.Status));
        Obj.AddPair('health', HealthStateName(Service.HealthState));
        Obj.AddPair('pid', TJSONNumber.Create(Service.ProcessId));
        Obj.AddPair('health_check_kind',
          HealthKindName(Service.Config.HealthCheck.Kind));
        if Service.Config.HealthCheck.Kind <> hckNone then
          Obj.AddPair('health_check_target',
            Service.Config.HealthCheck.Target);
        Arr.AddElement(Obj);
      end;
    finally
      FCriticalSection.Leave;
    end;
    Result := Arr.ToJSON;
  finally
    Arr.Free;
  end;
end;

function TServiceOrchestrator.HttpRenderHealth: string;
var
  Obj: TJSONObject;
begin
  Obj := TJSONObject.Create;
  try
    Obj.AddPair('status', 'ok');
    Obj.AddPair('orchestrator', 'running');
    Result := Obj.ToJSON;
  finally
    Obj.Free;
  end;
end;

function TServiceOrchestrator.HttpCheckReadiness(out AJson: string): Boolean;
var
  Obj: TJSONObject;
  Arr: TJSONArray;
  Item: TJSONObject;
  Service: TManagedService;
  ServiceName: string;
  AllReady: Boolean;
  ServiceReady: Boolean;
begin
  AllReady := True;
  Obj := TJSONObject.Create;
  try
    Arr := TJSONArray.Create;
    try
      FCriticalSection.Enter;
      try
        for ServiceName in FServices.Keys do
        begin
          Service := FServices[ServiceName];
          if not Service.Config.Enabled then
            Continue;

          ServiceReady := (Service.Status = ssRunning) and
                          (Service.HealthState = hsHealthy);
          if not ServiceReady then
            AllReady := False;

          Item := TJSONObject.Create;
          Item.AddPair('name', ServiceName);
          Item.AddPair('ready', TJSONBool.Create(ServiceReady));
          Item.AddPair('status', StatusName(Service.Status));
          Item.AddPair('health', HealthStateName(Service.HealthState));
          Arr.AddElement(Item);
        end;
      finally
        FCriticalSection.Leave;
      end;
    except
      Arr.Free;
      raise;
    end;
    Obj.AddPair('ready', TJSONBool.Create(AllReady));
    Obj.AddPair('services', Arr);
    AJson := Obj.ToJSON;
    Result := AllReady;
  finally
    Obj.Free;
  end;
end;

{ Management API }

function TServiceOrchestrator.AddService(const AName, ADisplayName, AExecutablePath: string;
  const AArguments, AWorkingDir: string): Boolean;
var
  Config: TServiceConfig;
  Service: TManagedService;
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

    Service := TManagedService.Create(Config, FLogger, FMetrics, FEventLog);
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
      if Assigned(FMetrics) then
        FMetrics.Forget(AName);
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

unit OrchaMetrics;

{ In-process metrics registry that renders Prometheus text exposition
  format (content type: text/plain; version=0.0.4). }

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.DateUtils,
  System.Generics.Collections;

type
  TMetricsRegistry = class
  private
    FLock: TCriticalSection;
    FStartTime: TDateTime;

    FChildStarts:    TDictionary<string, Int64>;
    FChildRestarts:  TDictionary<string, Int64>;
    FChildCrashes:   TDictionary<string, Int64>;
    FHealthFailures: TDictionary<string, Int64>;

    FChildUp:        TDictionary<string, Int64>;
    FChildHealthy:   TDictionary<string, Int64>;
    FChildPid:       TDictionary<string, Int64>;
    FLastStartEpoch: TDictionary<string, Int64>;

    procedure IncMap(const AMap: TDictionary<string, Int64>;
      const AKey: string; ADelta: Int64 = 1);
    procedure SetMap(const AMap: TDictionary<string, Int64>;
      const AKey: string; AValue: Int64);

    procedure EmitCounter(const ASB: TStringBuilder;
      const AName, AHelp: string;
      const AValues: TDictionary<string, Int64>);
    procedure EmitGauge(const ASB: TStringBuilder;
      const AName, AHelp: string;
      const AValues: TDictionary<string, Int64>);
  public
    constructor Create;
    destructor Destroy; override;

    procedure RecordStart(const AService: string);
    procedure RecordRestart(const AService: string);
    procedure RecordCrash(const AService: string);
    procedure RecordHealthFailure(const AService: string);

    procedure SetUp(const AService: string; AUp: Boolean);
    procedure SetHealthy(const AService: string; AHealthy: Boolean);
    procedure SetPid(const AService: string; APid: UInt32);

    procedure Forget(const AService: string);

    function Render: string;
  end;

function EscapeLabelValue(const S: string): string;

implementation

function EscapeLabelValue(const S: string): string;
var
  I: Integer;
  C: Char;
begin
  Result := '';
  for I := 1 to Length(S) do
  begin
    C := S[I];
    case C of
      '\': Result := Result + '\\';
      '"': Result := Result + '\"';
      #10: Result := Result + '\n';
      #13: ; // strip
    else
      Result := Result + C;
    end;
  end;
end;

{ TMetricsRegistry }

constructor TMetricsRegistry.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  FStartTime := Now;

  FChildStarts    := TDictionary<string, Int64>.Create;
  FChildRestarts  := TDictionary<string, Int64>.Create;
  FChildCrashes   := TDictionary<string, Int64>.Create;
  FHealthFailures := TDictionary<string, Int64>.Create;

  FChildUp        := TDictionary<string, Int64>.Create;
  FChildHealthy   := TDictionary<string, Int64>.Create;
  FChildPid       := TDictionary<string, Int64>.Create;
  FLastStartEpoch := TDictionary<string, Int64>.Create;
end;

destructor TMetricsRegistry.Destroy;
begin
  FLastStartEpoch.Free;
  FChildPid.Free;
  FChildHealthy.Free;
  FChildUp.Free;

  FHealthFailures.Free;
  FChildCrashes.Free;
  FChildRestarts.Free;
  FChildStarts.Free;

  FLock.Free;
  inherited;
end;

procedure TMetricsRegistry.IncMap(const AMap: TDictionary<string, Int64>;
  const AKey: string; ADelta: Int64);
var
  V: Int64;
begin
  if AMap.TryGetValue(AKey, V) then
    AMap[AKey] := V + ADelta
  else
    AMap.Add(AKey, ADelta);
end;

procedure TMetricsRegistry.SetMap(const AMap: TDictionary<string, Int64>;
  const AKey: string; AValue: Int64);
begin
  AMap.AddOrSetValue(AKey, AValue);
end;

procedure TMetricsRegistry.RecordStart(const AService: string);
begin
  FLock.Enter;
  try
    IncMap(FChildStarts, AService);
    SetMap(FLastStartEpoch, AService, DateTimeToUnix(Now, False));
    SetMap(FChildUp, AService, 1);
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.RecordRestart(const AService: string);
begin
  FLock.Enter;
  try
    IncMap(FChildRestarts, AService);
    SetMap(FLastStartEpoch, AService, DateTimeToUnix(Now, False));
    SetMap(FChildUp, AService, 1);
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.RecordCrash(const AService: string);
begin
  FLock.Enter;
  try
    IncMap(FChildCrashes, AService);
    SetMap(FChildUp, AService, 0);
    SetMap(FChildHealthy, AService, 0);
    SetMap(FChildPid, AService, 0);
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.RecordHealthFailure(const AService: string);
begin
  FLock.Enter;
  try
    IncMap(FHealthFailures, AService);
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.SetUp(const AService: string; AUp: Boolean);
begin
  FLock.Enter;
  try
    SetMap(FChildUp, AService, Ord(AUp));
    if not AUp then
    begin
      SetMap(FChildHealthy, AService, 0);
      SetMap(FChildPid, AService, 0);
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.SetHealthy(const AService: string; AHealthy: Boolean);
begin
  FLock.Enter;
  try
    SetMap(FChildHealthy, AService, Ord(AHealthy));
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.SetPid(const AService: string; APid: UInt32);
begin
  FLock.Enter;
  try
    SetMap(FChildPid, AService, APid);
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.Forget(const AService: string);
begin
  FLock.Enter;
  try
    FChildStarts.Remove(AService);
    FChildRestarts.Remove(AService);
    FChildCrashes.Remove(AService);
    FHealthFailures.Remove(AService);
    FChildUp.Remove(AService);
    FChildHealthy.Remove(AService);
    FChildPid.Remove(AService);
    FLastStartEpoch.Remove(AService);
  finally
    FLock.Leave;
  end;
end;

procedure TMetricsRegistry.EmitCounter(const ASB: TStringBuilder;
  const AName, AHelp: string; const AValues: TDictionary<string, Int64>);
var
  Pair: TPair<string, Int64>;
begin
  if AValues.Count = 0 then
    Exit;
  ASB.Append('# HELP ').Append(AName).Append(' ').Append(AHelp).Append(sLineBreak);
  ASB.Append('# TYPE ').Append(AName).Append(' counter').Append(sLineBreak);
  for Pair in AValues do
    ASB.Append(AName).Append('{service="')
       .Append(EscapeLabelValue(Pair.Key)).Append('"} ')
       .Append(Pair.Value).Append(sLineBreak);
end;

procedure TMetricsRegistry.EmitGauge(const ASB: TStringBuilder;
  const AName, AHelp: string; const AValues: TDictionary<string, Int64>);
var
  Pair: TPair<string, Int64>;
begin
  if AValues.Count = 0 then
    Exit;
  ASB.Append('# HELP ').Append(AName).Append(' ').Append(AHelp).Append(sLineBreak);
  ASB.Append('# TYPE ').Append(AName).Append(' gauge').Append(sLineBreak);
  for Pair in AValues do
    ASB.Append(AName).Append('{service="')
       .Append(EscapeLabelValue(Pair.Key)).Append('"} ')
       .Append(Pair.Value).Append(sLineBreak);
end;

function TMetricsRegistry.Render: string;
var
  SB: TStringBuilder;
  UptimeSec: Int64;
begin
  SB := TStringBuilder.Create;
  try
    FLock.Enter;
    try
      UptimeSec := SecondsBetween(Now, FStartTime);

      SB.Append('# HELP orcha_uptime_seconds Orchestrator uptime in seconds.').Append(sLineBreak);
      SB.Append('# TYPE orcha_uptime_seconds gauge').Append(sLineBreak);
      SB.Append('orcha_uptime_seconds ').Append(UptimeSec).Append(sLineBreak);

      EmitCounter(SB, 'orcha_child_starts_total',
        'Number of times a child has been started by the orchestrator.',
        FChildStarts);
      EmitCounter(SB, 'orcha_child_restarts_total',
        'Number of times a child has been restarted after a crash.',
        FChildRestarts);
      EmitCounter(SB, 'orcha_child_crashes_total',
        'Number of unexpected child exits.',
        FChildCrashes);
      EmitCounter(SB, 'orcha_health_probe_failures_total',
        'Number of failed health probes.',
        FHealthFailures);

      EmitGauge(SB, 'orcha_child_up',
        '1 if the child process is running, 0 otherwise.',
        FChildUp);
      EmitGauge(SB, 'orcha_child_healthy',
        '1 if the child passes health checks, 0 otherwise.',
        FChildHealthy);
      EmitGauge(SB, 'orcha_child_pid',
        'PID of the child process, or 0 if not running.',
        FChildPid);
      EmitGauge(SB, 'orcha_child_last_start_timestamp_seconds',
        'Unix timestamp of the most recent start of the child.',
        FLastStartEpoch);
    finally
      FLock.Leave;
    end;
    Result := SB.ToString;
  finally
    SB.Free;
  end;
end;

end.

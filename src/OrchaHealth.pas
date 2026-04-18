unit OrchaHealth;

{ Health-check primitives: TCP connect, HTTP GET, file existence.
  Each probe takes a timeout and returns a pass/fail with a human
  error message for logging. Uses Indy for transport since Indy is
  already required by the embedded HTTP metrics server. }

interface

uses
  Winapi.Windows,
  System.SysUtils,
  System.Classes,
  System.StrUtils,
  IdTCPClient,
  IdHTTP,
  IdGlobal;

type
  THealthCheckKind = (hckNone, hckTcp, hckHttp, hckFile);
  THealthState    = (hsUnknown, hsStarting, hsHealthy, hsUnhealthy);

  THealthCheckConfig = record
    Kind:                THealthCheckKind;
    Target:              string;
    IntervalSeconds:     Integer;
    TimeoutMs:           Integer;
    FailureThreshold:    Integer;
    StartupGraceSeconds: Integer;
    RestartOnUnhealthy:  Boolean;
    class function Defaults: THealthCheckConfig; static;
  end;

function ParseHealthKind(const S: string;
  ADefault: THealthCheckKind = hckNone): THealthCheckKind;
function HealthKindName(AKind: THealthCheckKind): string;
function HealthStateName(AState: THealthState): string;

function RunHealthProbe(const AConfig: THealthCheckConfig;
  out AErrorMsg: string): Boolean;

implementation

{ THealthCheckConfig }

class function THealthCheckConfig.Defaults: THealthCheckConfig;
begin
  Result.Kind                := hckNone;
  Result.Target              := '';
  Result.IntervalSeconds     := 30;
  Result.TimeoutMs           := 3000;
  Result.FailureThreshold    := 3;
  Result.StartupGraceSeconds := 10;
  Result.RestartOnUnhealthy  := False;
end;

function ParseHealthKind(const S: string;
  ADefault: THealthCheckKind): THealthCheckKind;
var
  U: string;
begin
  U := LowerCase(Trim(S));
  if U = 'tcp' then Result := hckTcp
  else if U = 'http' then Result := hckHttp
  else if U = 'file' then Result := hckFile
  else if (U = 'none') or (U = '') then Result := hckNone
  else Result := ADefault;
end;

function HealthKindName(AKind: THealthCheckKind): string;
begin
  case AKind of
    hckTcp:  Result := 'tcp';
    hckHttp: Result := 'http';
    hckFile: Result := 'file';
  else
    Result := 'none';
  end;
end;

function HealthStateName(AState: THealthState): string;
begin
  case AState of
    hsStarting:  Result := 'starting';
    hsHealthy:   Result := 'healthy';
    hsUnhealthy: Result := 'unhealthy';
  else
    Result := 'unknown';
  end;
end;

function ProbeTcp(const ATarget: string; ATimeoutMs: Integer;
  out AErr: string): Boolean;
var
  Client: TIdTCPClient;
  Parts: TArray<string>;
  Host: string;
  Port: Integer;
begin
  Result := False;
  AErr := '';

  Parts := SplitString(ATarget, ':');
  if Length(Parts) <> 2 then
  begin
    AErr := Format('invalid tcp target "%s" (expected host:port)', [ATarget]);
    Exit;
  end;

  Host := Trim(Parts[0]);
  if not TryStrToInt(Trim(Parts[1]), Port) or (Port < 1) or (Port > 65535) then
  begin
    AErr := Format('invalid tcp port "%s"', [Parts[1]]);
    Exit;
  end;

  Client := TIdTCPClient.Create(nil);
  try
    Client.Host := Host;
    Client.Port := Port;
    Client.ConnectTimeout := ATimeoutMs;
    Client.ReadTimeout := ATimeoutMs;
    try
      Client.Connect;
      try
        Result := Client.Connected;
      finally
        Client.Disconnect;
      end;
    except
      on E: Exception do
      begin
        AErr := E.Message;
        Result := False;
      end;
    end;
  finally
    Client.Free;
  end;
end;

function ProbeHttp(const AUrl: string; ATimeoutMs: Integer;
  out AErr: string): Boolean;
var
  Http: TIdHTTP;
  Code: Integer;
begin
  Result := False;
  AErr := '';

  Http := TIdHTTP.Create(nil);
  try
    Http.ConnectTimeout := ATimeoutMs;
    Http.ReadTimeout := ATimeoutMs;
    Http.HandleRedirects := True;
    try
      Http.Get(AUrl);
      Code := Http.ResponseCode;
      Result := (Code >= 200) and (Code < 300);
      if not Result then
        AErr := Format('http status %d', [Code]);
    except
      on E: Exception do
      begin
        Code := Http.ResponseCode;
        if (Code >= 200) and (Code < 300) then
          Result := True
        else
        begin
          AErr := E.Message;
          Result := False;
        end;
      end;
    end;
  finally
    Http.Free;
  end;
end;

function ProbeFile(const APath: string; out AErr: string): Boolean;
begin
  AErr := '';
  Result := FileExists(APath);
  if not Result then
    AErr := Format('file not found: %s', [APath]);
end;

function RunHealthProbe(const AConfig: THealthCheckConfig;
  out AErrorMsg: string): Boolean;
begin
  AErrorMsg := '';
  case AConfig.Kind of
    hckTcp:  Result := ProbeTcp(AConfig.Target, AConfig.TimeoutMs, AErrorMsg);
    hckHttp: Result := ProbeHttp(AConfig.Target, AConfig.TimeoutMs, AErrorMsg);
    hckFile: Result := ProbeFile(AConfig.Target, AErrorMsg);
    hckNone: Result := True;
  else
    Result := True;
  end;
end;

end.

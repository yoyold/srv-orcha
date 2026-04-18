unit OrchaHttp;

{ Embedded HTTP server exposing:
    GET /metrics   - Prometheus text exposition
    GET /healthz   - JSON liveness doc
    GET /readyz    - JSON readiness doc (503 if not ready)
    GET /services  - JSON list of managed services
    GET /          - tiny index

  Decoupled from the orchestrator via procedural callbacks so this
  unit does not need to know the TServiceOrchestrator type. }

interface

uses
  System.SysUtils,
  System.Classes,
  IdContext,
  IdCustomHTTPServer,
  IdHTTPServer;

type
  TRenderFunc    = reference to function: string;
  TReadinessFunc = reference to function(out AJson: string): Boolean;

  TMetricsServer = class
  private
    FServer: TIdHTTPServer;
    FPort: Integer;
    FBind: string;
    FRenderMetrics:  TRenderFunc;
    FRenderServices: TRenderFunc;
    FRenderHealth:   TRenderFunc;
    FReadiness:      TReadinessFunc;
    procedure HandleCommand(AContext: TIdContext;
      ARequestInfo: TIdHTTPRequestInfo;
      AResponseInfo: TIdHTTPResponseInfo);
    procedure SetText(AResponseInfo: TIdHTTPResponseInfo;
      const AContentType, AText: string);
  public
    constructor Create(APort: Integer; const ABind: string;
      ARenderMetrics, ARenderServices, ARenderHealth: TRenderFunc;
      AReadiness: TReadinessFunc);
    destructor Destroy; override;

    procedure Start;
    procedure Stop;

    property Port: Integer read FPort;
    property Bind: string read FBind;
  end;

implementation

uses
  IdGlobal,
  IdSocketHandle;

{ TMetricsServer }

constructor TMetricsServer.Create(APort: Integer; const ABind: string;
  ARenderMetrics, ARenderServices, ARenderHealth: TRenderFunc;
  AReadiness: TReadinessFunc);
var
  Binding: TIdSocketHandle;
begin
  inherited Create;
  FPort := APort;
  FBind := ABind;
  FRenderMetrics  := ARenderMetrics;
  FRenderServices := ARenderServices;
  FRenderHealth   := ARenderHealth;
  FReadiness      := AReadiness;

  FServer := TIdHTTPServer.Create(nil);
  FServer.DefaultPort := FPort;
  FServer.OnCommandGet := HandleCommand;

  Binding := FServer.Bindings.Add;
  if (FBind <> '') and not SameText(FBind, 'any') then
    Binding.IP := FBind
  else
    Binding.IP := '0.0.0.0';
  Binding.Port := FPort;
end;

destructor TMetricsServer.Destroy;
begin
  try
    Stop;
  except
    // ignore
  end;
  FServer.Free;
  inherited;
end;

procedure TMetricsServer.Start;
begin
  if not FServer.Active then
    FServer.Active := True;
end;

procedure TMetricsServer.Stop;
begin
  if FServer.Active then
    FServer.Active := False;
end;

procedure TMetricsServer.SetText(AResponseInfo: TIdHTTPResponseInfo;
  const AContentType, AText: string);
begin
  AResponseInfo.ContentType := AContentType;
  AResponseInfo.CharSet := 'utf-8';
  AResponseInfo.ContentText := AText;
end;

procedure TMetricsServer.HandleCommand(AContext: TIdContext;
  ARequestInfo: TIdHTTPRequestInfo;
  AResponseInfo: TIdHTTPResponseInfo);
var
  Path: string;
  Body: string;
  Ready: Boolean;
begin
  Path := LowerCase(ARequestInfo.Document);

  try
    if Path = '/metrics' then
    begin
      SetText(AResponseInfo, 'text/plain; version=0.0.4', FRenderMetrics());
      AResponseInfo.ResponseNo := 200;
    end
    else if Path = '/healthz' then
    begin
      SetText(AResponseInfo, 'application/json', FRenderHealth());
      AResponseInfo.ResponseNo := 200;
    end
    else if Path = '/readyz' then
    begin
      Ready := FReadiness(Body);
      SetText(AResponseInfo, 'application/json', Body);
      if Ready then
        AResponseInfo.ResponseNo := 200
      else
        AResponseInfo.ResponseNo := 503;
    end
    else if Path = '/services' then
    begin
      SetText(AResponseInfo, 'application/json', FRenderServices());
      AResponseInfo.ResponseNo := 200;
    end
    else if (Path = '/') or (Path = '') then
    begin
      SetText(AResponseInfo, 'text/plain; charset=utf-8',
        'srv-orcha'#10 +
        '  GET /metrics   Prometheus text exposition'#10 +
        '  GET /healthz   liveness (JSON)'#10 +
        '  GET /readyz    readiness (JSON, 503 if not ready)'#10 +
        '  GET /services  service list (JSON)'#10);
      AResponseInfo.ResponseNo := 200;
    end
    else
    begin
      SetText(AResponseInfo, 'text/plain', 'not found');
      AResponseInfo.ResponseNo := 404;
    end;
  except
    on E: Exception do
    begin
      SetText(AResponseInfo, 'text/plain',
        'internal error: ' + E.ClassName + ': ' + E.Message);
      AResponseInfo.ResponseNo := 500;
    end;
  end;
end;

end.

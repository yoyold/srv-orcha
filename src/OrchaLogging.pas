unit OrchaLogging;

{ Structured JSON-lines logger with size-based rotation, plus a thin
  wrapper around the Windows Event Log API. All public methods are
  thread-safe and swallow their own errors so logging never crashes the
  hosting service. }

interface

uses
  Winapi.Windows,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.JSON,
  System.DateUtils,
  System.IOUtils;

type
  TLogLevel = (llDebug, llInfo, llWarn, llError);

  TLogField = record
    Key: string;
    Value: string;
  end;

  TStructuredLogger = class
  private
    FLock: TCriticalSection;
    FLogPath: string;
    FMinLevel: TLogLevel;
    FMaxFileSizeBytes: Int64;
    FMaxFiles: Integer;
    procedure RotateIfNeeded;
    function BuildLine(ALevel: TLogLevel;
      const AService, AEvent, AMessage, ATraceID: string;
      const AFields: array of TLogField): string;
  public
    constructor Create(const ALogPath: string; AMinLevel: TLogLevel;
      AMaxFileSizeBytes: Int64; AMaxFiles: Integer);
    destructor Destroy; override;

    procedure Log(ALevel: TLogLevel;
      const AService, AEvent, AMessage: string); overload;
    procedure Log(ALevel: TLogLevel;
      const AService, AEvent, AMessage, ATraceID: string;
      const AFields: array of TLogField); overload;

    property MinLevel: TLogLevel read FMinLevel write FMinLevel;
  end;

  TEventLogWriter = class
  private
    FHandle: THandle;
    FSourceName: string;
    FEnabled: Boolean;
    procedure Emit(AType: Word; const AMessage: string);
  public
    constructor Create(const ASourceName: string; AEnabled: Boolean);
    destructor Destroy; override;
    procedure Info(const AMessage: string);
    procedure Warn(const AMessage: string);
    procedure Error(const AMessage: string);
  end;

function LogLevelName(ALevel: TLogLevel): string;
function ParseLogLevel(const S: string; ADefault: TLogLevel = llInfo): TLogLevel;
function MakeField(const AKey, AValue: string): TLogField;
function NewTraceID: string;

implementation

const
  EVENTLOG_SUCCESS_TYPE     = $0000;
  EVENTLOG_ERROR_TYPE       = $0001;
  EVENTLOG_WARNING_TYPE     = $0002;
  EVENTLOG_INFORMATION_TYPE = $0004;

function LogLevelName(ALevel: TLogLevel): string;
begin
  case ALevel of
    llDebug: Result := 'DEBUG';
    llInfo:  Result := 'INFO';
    llWarn:  Result := 'WARN';
    llError: Result := 'ERROR';
  else
    Result := '?';
  end;
end;

function ParseLogLevel(const S: string; ADefault: TLogLevel): TLogLevel;
var
  U: string;
begin
  U := UpperCase(Trim(S));
  if U = 'DEBUG' then Result := llDebug
  else if U = 'INFO' then Result := llInfo
  else if U = 'WARN' then Result := llWarn
  else if (U = 'ERROR') or (U = 'ERR') then Result := llError
  else Result := ADefault;
end;

function MakeField(const AKey, AValue: string): TLogField;
begin
  Result.Key := AKey;
  Result.Value := AValue;
end;

function NewTraceID: string;
var
  G: TGUID;
  S: string;
begin
  if CreateGUID(G) <> S_OK then
  begin
    Result := IntToHex(GetTickCount, 8) + IntToHex(Random($FFFFFFFF), 8);
    Exit;
  end;
  S := GUIDToString(G);
  // Strip braces and dashes -> 32 hex chars (compact W3C-ish id)
  S := StringReplace(S, '{', '', [rfReplaceAll]);
  S := StringReplace(S, '}', '', [rfReplaceAll]);
  S := StringReplace(S, '-', '', [rfReplaceAll]);
  Result := LowerCase(S);
end;

{ TStructuredLogger }

constructor TStructuredLogger.Create(const ALogPath: string;
  AMinLevel: TLogLevel; AMaxFileSizeBytes: Int64; AMaxFiles: Integer);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FLogPath := ALogPath;
  FMinLevel := AMinLevel;

  if AMaxFileSizeBytes <= 0 then
    FMaxFileSizeBytes := 10 * 1024 * 1024
  else
    FMaxFileSizeBytes := AMaxFileSizeBytes;

  if AMaxFiles <= 0 then
    FMaxFiles := 7
  else
    FMaxFiles := AMaxFiles;

  try
    ForceDirectories(ExtractFilePath(FLogPath));
  except
    // best effort
  end;
end;

destructor TStructuredLogger.Destroy;
begin
  FLock.Free;
  inherited;
end;

function TStructuredLogger.BuildLine(ALevel: TLogLevel;
  const AService, AEvent, AMessage, ATraceID: string;
  const AFields: array of TLogField): string;
var
  Obj: TJSONObject;
  I: Integer;
begin
  Obj := TJSONObject.Create;
  try
    Obj.AddPair('ts', FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz', Now));
    Obj.AddPair('level', LogLevelName(ALevel));
    if AService <> '' then Obj.AddPair('service', AService);
    if AEvent <> '' then Obj.AddPair('event', AEvent);
    if AMessage <> '' then Obj.AddPair('msg', AMessage);
    if ATraceID <> '' then Obj.AddPair('trace_id', ATraceID);
    for I := 0 to High(AFields) do
      Obj.AddPair(AFields[I].Key, AFields[I].Value);
    Result := Obj.ToJSON + sLineBreak;
  finally
    Obj.Free;
  end;
end;

procedure TStructuredLogger.RotateIfNeeded;
var
  Size: Int64;
  I: Integer;
  Src, Dst: string;
begin
  if not FileExists(FLogPath) then
    Exit;

  try
    Size := TFile.GetSize(FLogPath);
  except
    Exit;
  end;

  if Size < FMaxFileSizeBytes then
    Exit;

  // Drop the oldest
  Dst := FLogPath + '.' + IntToStr(FMaxFiles);
  if FileExists(Dst) then
    DeleteFile(PChar(Dst));

  // Shift .N -> .N+1
  for I := FMaxFiles - 1 downto 1 do
  begin
    Src := FLogPath + '.' + IntToStr(I);
    Dst := FLogPath + '.' + IntToStr(I + 1);
    if FileExists(Src) then
      RenameFile(Src, Dst);
  end;

  // Current log becomes .1
  RenameFile(FLogPath, FLogPath + '.1');
end;

procedure TStructuredLogger.Log(ALevel: TLogLevel;
  const AService, AEvent, AMessage: string);
var
  Empty: array of TLogField;
begin
  SetLength(Empty, 0);
  Log(ALevel, AService, AEvent, AMessage, '', Empty);
end;

procedure TStructuredLogger.Log(ALevel: TLogLevel;
  const AService, AEvent, AMessage, ATraceID: string;
  const AFields: array of TLogField);
var
  Line: string;
  Bytes: TBytes;
  Stream: TFileStream;
begin
  if ALevel < FMinLevel then
    Exit;

  Line := BuildLine(ALevel, AService, AEvent, AMessage, ATraceID, AFields);
  Bytes := TEncoding.UTF8.GetBytes(Line);

  FLock.Enter;
  try
    try
      RotateIfNeeded;

      if FileExists(FLogPath) then
        Stream := TFileStream.Create(FLogPath,
          fmOpenWrite or fmShareDenyWrite)
      else
        Stream := TFileStream.Create(FLogPath,
          fmCreate or fmShareDenyWrite);
      try
        Stream.Seek(0, soEnd);
        if Length(Bytes) > 0 then
          Stream.WriteBuffer(Bytes[0], Length(Bytes));
      finally
        Stream.Free;
      end;
    except
      // never let logging crash the host
    end;
  finally
    FLock.Leave;
  end;
end;

{ TEventLogWriter }

constructor TEventLogWriter.Create(const ASourceName: string; AEnabled: Boolean);
begin
  inherited Create;
  FSourceName := ASourceName;
  FEnabled := AEnabled;
  FHandle := 0;
  if FEnabled then
  try
    FHandle := RegisterEventSource(nil, PChar(FSourceName));
  except
    FHandle := 0;
  end;
end;

destructor TEventLogWriter.Destroy;
begin
  if FHandle <> 0 then
  try
    DeregisterEventSource(FHandle);
  except
    // ignore
  end;
  inherited;
end;

procedure TEventLogWriter.Emit(AType: Word; const AMessage: string);
var
  PMsg: PChar;
begin
  if (FHandle = 0) or not FEnabled then
    Exit;
  PMsg := PChar(AMessage);
  try
    ReportEvent(FHandle, AType, 0, 0, nil, 1, 0, @PMsg, nil);
  except
    // ignore
  end;
end;

procedure TEventLogWriter.Info(const AMessage: string);
begin
  Emit(EVENTLOG_INFORMATION_TYPE, AMessage);
end;

procedure TEventLogWriter.Warn(const AMessage: string);
begin
  Emit(EVENTLOG_WARNING_TYPE, AMessage);
end;

procedure TEventLogWriter.Error(const AMessage: string);
begin
  Emit(EVENTLOG_ERROR_TYPE, AMessage);
end;

end.

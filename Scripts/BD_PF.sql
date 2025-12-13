USE master;
GO

-- 1) Crear base de datos
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'GlobalPointsWatcher')
BEGIN
    CREATE DATABASE GlobalPointsWatcher
END
GO

USE GlobalPointsWatcher;
GO

-- 2. DDL - CREACIÓN DE TABLAS

-- Seguridad para credenciales (encriptaci�n sim�trica)
-- NOTA: Cambia 'StrongMasterKeyPassword!' por una contraseña robusta guardada de forma segura.
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'StrongMasterKeyPassword!';
CREATE CERTIFICATE EmailCredsCert WITH SUBJECT = 'Certificado para credenciales de correo';
CREATE SYMMETRIC KEY EmailCredsKey WITH ALGORITHM = AES_256 ENCRYPTION BY CERTIFICATE EmailCredsCert;
GO

-- Tabla de Configuración
IF OBJECT_ID('dbo.Parameters', 'U') IS NULL
CREATE TABLE dbo.Parameters (
    ParamKey NVARCHAR(50) PRIMARY KEY,
    ParamValue NVARCHAR(MAX) NOT NULL
);
GO

-- TABLA DE USUARIOS AUTORIZADOS
IF OBJECT_ID('dbo.AppUsers', 'U') IS NULL
CREATE TABLE dbo.AppUsers (
    UserId INT IDENTITY(1,1) PRIMARY KEY,
    TelegramChatId BIGINT NOT NULL UNIQUE, -- El ID de Telegram
    FirstName NVARCHAR(100) NULL,
    IsActive BIT DEFAULT 1,
    RegisteredAt DATETIME DEFAULT GETDATE()
);
GO

-- Tabla de credenciales de correo (usuario + contraseña encriptada)
IF OBJECT_ID('dbo.EmailCredentials', 'U') IS NULL
CREATE TABLE dbo.EmailCredentials (
    Id               INT IDENTITY(1,1) PRIMARY KEY,
    AppUserId INT NOT NULL,
    Email            NVARCHAR(256) NOT NULL UNIQUE,
    PasswordEncrypted VARBINARY(MAX) NOT NULL,
    UpdatedAt        DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Email_User FOREIGN KEY (AppUserId) REFERENCES dbo.AppUsers(UserId),
    CONSTRAINT UQ_Email UNIQUE(Email),
    CONSTRAINT UQ_AppUserId UNIQUE(AppUserId), 
);
GO

-- Tabla de Categorías (Transporte, Comida, Servicios...)
IF OBJECT_ID('dbo.Categories', 'U') IS NULL
CREATE TABLE dbo.Categories (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(50) NOT NULL UNIQUE
);

IF NOT EXISTS (SELECT 1 FROM dbo.Categories WHERE Name = 'General')
BEGIN
    INSERT INTO dbo.Categories (Name) VALUES ('General');
END

IF OBJECT_ID('dbo.Comercio', 'U') IS NULL
CREATE TABLE dbo.Comercio (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    [Name] NVARCHAR(200) NOT NULL UNIQUE, 
    CategoryId INT NOT NULL DEFAULT 1, 

    CONSTRAINT FK_Comercio_Categories FOREIGN KEY (CategoryId) REFERENCES dbo.Categories(Id)
);
GO

-- Tabla de Tarjetas de Usuarios (Relación Telegram - Tarjeta)
IF OBJECT_ID('dbo.UserCards', 'U') IS NULL
CREATE TABLE dbo.UserCards (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    AppUserId INT NOT NULL,
    ChatId BIGINT NOT NULL,
    CardLast4 CHAR(4) NOT NULL,
    Bank  NVARCHAR(100) NOT NULL,
    Alias NVARCHAR(50) NULL,
    CreatedAt DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_UserCards_Users FOREIGN KEY (AppUserId) REFERENCES dbo.AppUsers(UserId),
    CONSTRAINT UQ_Chat_Card UNIQUE(AppUserId, CardLast4)
);
GO

IF OBJECT_ID('dbo.ComercioReglaUsuario', 'U') IS NULL
CREATE TABLE dbo.ComercioReglaUsuario (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    ComercioId INT NOT NULL,
    UserCardId INT NOT NULL,
    Multiplicador DECIMAL(4,2) NOT NULL DEFAULT 1.0,
    LastUpdated DATETIME2(0) DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT FK_Rule_Comercio FOREIGN KEY (ComercioId) REFERENCES dbo.Comercio(Id),
    CONSTRAINT FK_Rule_Card FOREIGN KEY (UserCardId) REFERENCES dbo.UserCards(Id),
    CONSTRAINT UQ_Comercio_Card UNIQUE (ComercioId, UserCardId) 
);
GO

-- Tabla de transacciones
IF OBJECT_ID('dbo.Transactions', 'U') IS NULL
CREATE TABLE dbo.Transactions (
    Id            BIGINT IDENTITY(1,1) PRIMARY KEY,
    UserCardId INT NOT NULL,
    ComercioId INT NOT NULL,
    CardLast4     CHAR(4)       NOT NULL,
    AmountUSD     DECIMAL(12,2) NOT NULL,
    Points        INT           NOT NULL,
	Multiplicador DECIMAL(4,2) NULL,
    TransactionAt DATETIME2(0)  NOT NULL,
    CreatedAt     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Trans_Card FOREIGN KEY (UserCardId) REFERENCES dbo.UserCards(Id),
    CONSTRAINT FK_Trans_Comercio FOREIGN KEY (ComercioId) REFERENCES dbo.Comercio(Id)
);

CREATE INDEX IX_Transactions_TransactionAt ON dbo.Transactions(TransactionAt);
CREATE INDEX IX_Transactions_ComercioMonth ON dbo.Transactions(ComercioId, TransactionAt);
CREATE INDEX IX_Transactions_CardLast4Month ON dbo.Transactions(CardLast4, TransactionAt);
GO

-- Tabla de Auditoría de Validaciones (Bot)
IF OBJECT_ID('dbo.AuditoriaValidaciones', 'U') IS NULL
CREATE TABLE dbo.AuditoriaValidaciones (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TokenAuditoria NVARCHAR(128),
    Estado NVARCHAR(20) NOT NULL CHECK (Estado IN ('RECONOCIDA', 'NO RECONOCIDA')),
    FechaValidacion DATETIME DEFAULT GETDATE()
);
GO

-- Tabla de Log de Errores
IF OBJECT_ID('dbo.ErrorLog', 'U') IS NULL
CREATE TABLE dbo.ErrorLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    ErrorTime DATETIME DEFAULT GETDATE(),
    ErrorMessage NVARCHAR(MAX),
    StoredProcedure NVARCHAR(100)
);
GO

-- Procedimiento para Registrar/Login
CREATE OR ALTER PROCEDURE dbo.sp_RegisterUser
    @ChatId BIGINT,
    @FirstName NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @UserId INT;

    IF EXISTS (SELECT 1 FROM dbo.AppUsers WHERE TelegramChatId = @ChatId)
    BEGIN
        -- A) EL USUARIO YA EXISTE: Actualizamos sus datos (por si cambió de nombre)
        UPDATE dbo.AppUsers 
        SET FirstName = @FirstName, 
            IsActive = 1
        WHERE TelegramChatId = @ChatId;

        -- Recuperamos su ID
        SELECT @UserId = UserId FROM dbo.AppUsers WHERE TelegramChatId = @ChatId;

        SELECT 'EXISTING_USER' AS Status, @UserId AS UserId;
    END
    ELSE
    BEGIN
        -- B) EL USUARIO ES NUEVO: Lo insertamos
        INSERT INTO dbo.AppUsers (TelegramChatId, FirstName)
        VALUES (@ChatId, @FirstName);

        -- Recuperamos el ID recién creado
        SET @UserId = SCOPE_IDENTITY();

        SELECT 'NEW_USER' AS Status, @UserId AS UserId;
    END
END;
GO

-- Procedimiento para insertar credenciales (encripta la contraseña)
CREATE OR ALTER PROCEDURE dbo.sp_SaveEmailCredential
    @Email NVARCHAR(256),
    @PlainPassword NVARCHAR(4000)
AS
BEGIN
    SET NOCOUNT ON;
    OPEN SYMMETRIC KEY EmailCredsKey DECRYPTION BY CERTIFICATE EmailCredsCert;

    DECLARE @Enc VARBINARY(MAX) = EncryptByKey(Key_GUID('EmailCredsKey'), CONVERT(VARBINARY(MAX), @PlainPassword));

    MERGE dbo.EmailCredentials AS target
    USING (SELECT @Email AS Email) AS src
    ON target.Email = src.Email
    WHEN MATCHED THEN
        UPDATE SET PasswordEncrypted = @Enc, UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (Email, PasswordEncrypted) VALUES (@Email, @Enc);

    CLOSE SYMMETRIC KEY EmailCredsKey;
END;
GO

-- Procedimiento para insertar transacción 
CREATE OR ALTER PROCEDURE dbo.sp_InsertTransactionFromEmail
    @RawComercioTexto NVARCHAR(200),
    @CardLast4 CHAR(4),
    @AmountUSD DECIMAL(12,2),
    -- Salidas para Bot
    @TransactionId INT OUTPUT,
    @BotAction VARCHAR(20) OUTPUT, -- 'AUTO', 'ASK_MULT', 'ASK_CAT', 'ASK_BOTH'
    @MessageText NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ComercioId INT, @UserCardId INT, @CategoryId INT;
    DECLARE @StoredMultiplier DECIMAL(4,2);
    DECLARE @RawComercioLimpio NVARCHAR(200) = LTRIM(RTRIM(@RawComercioTexto));
    DECLARE @Points INT;

    BEGIN TRANSACTION;

    -- 1. Gestionar Comercio
    SELECT @ComercioId = Id, @CategoryId = CategoryId FROM dbo.Comercio WHERE Name = @RawComercioLimpio;

    IF @ComercioId IS NULL
    BEGIN
        INSERT INTO dbo.Comercio (Name) VALUES (@RawComercioLimpio);
        SET @ComercioId = SCOPE_IDENTITY();
    END

    -- 2. Gestionar Tarjeta
    SELECT TOP 1 @UserCardId = Id FROM dbo.UserCards WHERE CardLast4 = @CardLast4;
    IF @UserCardId IS NULL THROW 51000, 'Tarjeta no encontrada', 1;

    -- 3. Buscar regla de acumulación
    SELECT @StoredMultiplier = Multiplicador
    FROM dbo.ComercioReglaUsuario
    WHERE ComercioId = @ComercioId AND UserCardId = @UserCardId;

    -- 4. Lógica
    IF @StoredMultiplier IS NOT NULL
    BEGIN
        --- ESCENARIO: AUTOMÁTICO ---
        SET @Points = CAST((@AmountUSD * @StoredMultiplier) AS INT);
        
        INSERT INTO dbo.Transactions (UserCardId, ComercioId, CardLast4, AmountUSD, Points, TransactionAt, Multiplicador)
        VALUES (@UserCardId, @ComercioId, @CardLast4, @AmountUSD, @Points, SYSUTCDATETIME(), @StoredMultiplier);
        
        SET @TransactionId = SCOPE_IDENTITY();
        
        -- Verificar si nos falta la categoría para los reportes
        IF @CategoryId IS NULL
        BEGIN
            SET @BotAction = 'ASK_CAT'; -- Ya tengo los puntos, pero dime qué es para el Reporte
            SET @MessageText = CONCAT('✅ ', @Points, ' pts agregados (x', @StoredMultiplier, '). Pero, ¿qué categoría es ', @RawComercioLimpio, '?');
        END
        ELSE
        BEGIN
            SET @BotAction = 'AUTO'; -- Todo perfecto
            SET @MessageText = CONCAT('✅ ', @Points, ' pts agregados en ', @RawComercioLimpio, ' (x', @StoredMultiplier, ').');
        END
    END
    ELSE
    BEGIN
        --- ESCENARIO: NUEVO / MANUAL ---
        -- Insertamos pendiente (0 puntos)
        INSERT INTO dbo.Transactions (UserCardId, ComercioId, CardLast4, AmountUSD, Points, TransactionAt)
        VALUES (@UserCardId, @ComercioId, @CardLast4, @AmountUSD, 0, SYSUTCDATETIME());
        
        SET @TransactionId = SCOPE_IDENTITY();
        
        IF @CategoryId IS NULL
             SET @BotAction = 'ASK_BOTH'; -- No sé ni puntos ni categoría
        ELSE
             SET @BotAction = 'ASK_MULT'; -- Sé la categoría, pero no sé cuántos puntos da esta transacción
             
        SET @MessageText = CONCAT('❓ Nueva compra en ', @RawComercioLimpio, ' ($', @AmountUSD, '). Configuración requerida.');
    END

    COMMIT TRANSACTION;
END;
GO

CREATE OR ALTER PROCEDURE sp_CompletarConfiguracion
    @TransactionId INT,
    @SelectedMultiplier DECIMAL(4,2), -- NULL si solo estamos actualizando categoría
    @SelectedCategoryName NVARCHAR(50) -- NULL si ya la teníamos
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ComercioId INT, @UserCardId INT, @AmountUSD DECIMAL(12,2);
    DECLARE @NewCategoryId INT;

    BEGIN TRANSACTION;

    SELECT @ComercioId = ComercioId, @UserCardId = UserCardId, @AmountUSD = AmountUSD
    FROM dbo.Transactions WHERE Id = @TransactionId;

    -- 1. Actualizar Categoría (Si el usuario la envió)
    IF @SelectedCategoryName IS NOT NULL
    BEGIN
        -- Buscar o Crear Categoría
        SELECT @NewCategoryId = Id FROM dbo.Categories WHERE Name = @SelectedCategoryName;
        IF @NewCategoryId IS NULL
        BEGIN
            INSERT INTO dbo.Categories (Name) VALUES (@SelectedCategoryName);
            SET @NewCategoryId = SCOPE_IDENTITY();
        END

        UPDATE dbo.Comercio SET CategoryId = @NewCategoryId WHERE Id = @ComercioId;
    END

    -- 2. Actualizar Puntos y Regla (Si el usuario envió multiplicador)
    IF @SelectedMultiplier IS NOT NULL
    BEGIN
        -- 1. Actualizar Transacción Actual
        UPDATE dbo.Transactions
        SET Multiplicador = @SelectedMultiplier,
            Points = CAST((AmountUSD * @SelectedMultiplier) AS INT)
        WHERE Id = @TransactionId;

        MERGE dbo.ComercioReglaUsuario AS target
        USING (SELECT @ComercioId AS CId, @UserCardId AS UId) AS source
        ON (target.ComercioId = source.CId AND target.UserCardId = source.UId)
        WHEN MATCHED THEN
            UPDATE SET Multiplicador = @SelectedMultiplier, LastUpdated = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (ComercioId, UserCardId, Multiplicador)
            VALUES (@ComercioId, @UserCardId, @SelectedMultiplier);

    END

    COMMIT TRANSACTION;
END;
GO
-- 7) Resumen mensual por tarjeta
CREATE OR ALTER PROCEDURE dbo.sp_MonthlyPointsSummary
    @Year INT,
    @Month INT,
    @CardLast4 CHAR(4)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartDate DATETIME2(0) = DATEFROMPARTS(@Year, @Month, 1);
    DECLARE @EndDate DATETIME2(0) = DATEADD(MONTH, 1, @StartDate);

    -- Usamos una CTE para filtrar primero
    ;WITH MonthlyData AS (
        SELECT 
            t.AmountUSD, 
            t.Points, 
            t.ComercioId
        FROM dbo.Transactions t
        INNER JOIN dbo.UserCards c ON t.UserCardId = c.Id
        WHERE t.TransactionAt >= @StartDate 
          AND t.TransactionAt < @EndDate
          AND c.CardLast4 = @CardLast4
    )
    SELECT
        ISNULL(SUM(AmountUSD), 0.00) AS TotalAmountUSD,
        ISNULL(SUM(Points), 0) AS TotalPoints,
        (
            SELECT TOP 1 C.Name
            FROM MonthlyData md
            INNER JOIN dbo.Comercio C ON md.ComercioId = C.Id
            GROUP BY C.Name
            ORDER BY COUNT(*) DESC, SUM(md.AmountUSD) DESC
        ) AS TopComercio, -- El NOMBRE del comercio más frecuente
        (
            SELECT TOP 1 Cat.Name
            FROM MonthlyData md
            INNER JOIN dbo.Comercio C ON md.ComercioId = C.Id
            LEFT JOIN dbo.Categories Cat ON C.CategoryId = Cat.Id
            GROUP BY Cat.Name
            ORDER BY SUM(md.AmountUSD) DESC
        ) AS TopCategory -- Nombre de la categoría donde más gastaste
    FROM MonthlyData;
END;
GO

-- 8) Env�o por correo (Database Mail) del resumen mensual
-- Requiere perfil y cuenta de Database Mail (ver secci�n siguiente)
CREATE OR ALTER PROCEDURE dbo.sp_SendMonthlySummaryEmail
    @CardLast4 CHAR(4),
    @SendTo NVARCHAR(256),
    @ProfileName NVARCHAR(128) = N'DefaultProfile' -- cambia por tu perfil real
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Year INT = YEAR(GETDATE());
    DECLARE @Month INT = MONTH(GETDATE());

    DECLARE @TotalPoints INT;
    DECLARE @TotalAmount DECIMAL(18,2);
    DECLARE @TopCompany NVARCHAR(200);

    SELECT @TotalPoints = SUM(Points),
           @TotalAmount = SUM(AmountUSD),
           @TopCompany = (SELECT TOP 1 b.Name Compania
                          FROM dbo.Transactions a
						  JOIN dbo.Comercio b on a.ComercioId = b.Id
                          WHERE YEAR(TransactionAt) = @Year AND MONTH(TransactionAt) = @Month AND CardLast4 = @CardLast4
                          GROUP BY b.Name
                          ORDER BY COUNT(*) DESC, SUM(AmountUSD) DESC)
    FROM dbo.Transactions
    WHERE YEAR(TransactionAt) = @Year AND MONTH(TransactionAt) = @Month AND CardLast4 = @CardLast4;

    DECLARE @Body NVARCHAR(MAX) = CONCAT(
        'Resumen mensual ', FORMAT(DATEFROMPARTS(@Year, @Month, 1), 'MMMM yyyy'), CHAR(13)+CHAR(10),
        'Tarjeta terminaci�n: ', @CardLast4, CHAR(13)+CHAR(10),
        'Total puntos: ', COALESCE(CONVERT(NVARCHAR(50), @TotalPoints), N'0'), CHAR(13)+CHAR(10),
        'Equivalente USD: $', COALESCE(CONVERT(NVARCHAR(50), CAST(COALESCE(@TotalPoints,0)/100.0 AS DECIMAL(18,2))), N'0.00'), CHAR(13)+CHAR(10),
        'Comercio m�s consumido: ', COALESCE(@TopCompany, N'�'), CHAR(13)+CHAR(10)
    );

    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = @ProfileName,
        @recipients   = @SendTo,
        @subject      = CONCAT('Resumen puntos ', FORMAT(DATEFROMPARTS(@Year, @Month, 1), 'MMMM yyyy')),
        @body         = @Body;
END;
GO

-- 9) Procedimiento envoltorio para fin de mes (solo env�a si hoy es �ltimo d�a)
CREATE OR ALTER PROCEDURE dbo.sp_SendMonthlyIfLastDay
    @CardLast4 CHAR(4),
    @SendTo NVARCHAR(256),
    @ProfileName NVARCHAR(128) = N'DefaultProfile'
AS
BEGIN
    IF CAST(GETDATE() AS DATE) = EOMONTH(GETDATE())
        EXEC dbo.sp_SendMonthlySummaryEmail @CardLast4=@CardLast4, @SendTo=@SendTo, @ProfileName=@ProfileName;
END;
GO

-- Cargar los datos de correo SMTP
USE GlobalPointsWatcher;
GO
EXEC dbo.sp_SaveEmailCredential
    @Email = N'buglione2500@gmail.com',
    @PlainPassword = N'tsvk jljb torw blih';


-- Habilitar Database Mail si est� deshabilitado
EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
EXEC sp_configure 'Database Mail XPs', 1; RECONFIGURE;

-- Crear cuenta
EXEC msdb.dbo.sysmail_add_account_sp
    @account_name = 'CuentaSMTP',
    @description  = 'Cuenta SMTP para res�menes',
    @email_address= 'buglione2500@gmail.com',
    @display_name = 'GlobalPoints Watcher',
    @mailserver_name = 'smtp.gmail.com', -- servidor SMTP
    @port = 587,
    @enable_ssl = 1,
    @username = 'buglione2500@gmail.com',
    @password = 'tsvk jljb torw blih';

-- Crear perfil
EXEC msdb.dbo.sysmail_add_profile_sp
    @profile_name = 'DefaultProfile',
    @description  = 'Perfil por defecto';

-- Asociar cuenta al perfil
EXEC msdb.dbo.sysmail_add_profileaccount_sp
    @profile_name = 'DefaultProfile',
    @account_name = 'CuentaSMTP',
    @sequence_number = 1;

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'DefaultProfile',
    @recipients = 'buglione2500@gmail.com',
    @subject = 'Prueba Database Mail',
    @body = 'Mensaje de prueba';

USE msdb;
GO

DECLARE @jobId UNIQUEIDENTIFIER;

EXEC sp_add_job @job_name = N'Enviar resumen mensual puntos',
                @enabled = 1,
                @description = N'Env�a el resumen de puntos el �ltimo d�a de cada mes',
                @start_step_id = 1,
                @owner_login_name = N'sa', -- ajusta el owner
                @job_id = @jobId OUTPUT;

EXEC sp_add_jobstep
    @job_id = @jobId,
    @step_id = 1,
    @step_name = N'Ejecutar resumen si es fin de mes',
    @subsystem = N'TSQL',
    @database_name = N'GlobalPointsWatcher',
    @command = N'EXEC dbo.sp_SendMonthlyIfLastDay @CardLast4=''8624'', @SendTo=''buglione2500@gmail.com'', @ProfileName=''DefaultProfile'';',
    @on_success_action = 1, -- Quit
    @on_fail_action = 2;    -- Retry/quit

-- Programa diario 23:59, el procedimiento s�lo enviar� si es fin de mes
EXEC sp_add_schedule
    @schedule_name = N'Diario 23:59',
    @freq_type = 4, -- diario
    @freq_interval = 1,
    @active_start_time = 235900; -- 23:59:00

EXEC sp_attach_schedule @job_id = @jobId, @schedule_name = N'Diario 23:59';
EXEC sp_add_jobserver  @job_id = @jobId, @server_name = N'(local)'; -- ajusta el nombre del servidor si aplica
GO
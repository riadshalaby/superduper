@ECHO OFF
SETLOCAL

SET "BASE_DIR=%~dp0"
IF "%BASE_DIR:~-1%"=="\" SET "BASE_DIR=%BASE_DIR:~0,-1%"
SET "WRAPPER_DIR=%BASE_DIR%\.mvn\wrapper"
SET "WRAPPER_JAR=%WRAPPER_DIR%\maven-wrapper.jar"
SET "WRAPPER_PROPERTIES=%WRAPPER_DIR%\maven-wrapper.properties"

IF DEFINED JAVA_HOME (
  SET "JAVACMD=%JAVA_HOME%\bin\java.exe"
) ELSE (
  SET "JAVACMD=java"
)

IF NOT EXIST "%WRAPPER_JAR%" (
  IF NOT EXIST "%WRAPPER_PROPERTIES%" (
    ECHO Error: Missing "%WRAPPER_PROPERTIES%"
    EXIT /B 1
  )

  IF NOT EXIST "%WRAPPER_DIR%" MKDIR "%WRAPPER_DIR%"
  ECHO Downloading Maven wrapper from https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.2/maven-wrapper-3.3.2.jar
  powershell -NoProfile -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -UseBasicParsing -Uri 'https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.2/maven-wrapper-3.3.2.jar' -OutFile '%WRAPPER_JAR%'"
  IF ERRORLEVEL 1 (
    ECHO Error: Failed to download Maven wrapper jar.
    EXIT /B 1
  )
)

"%JAVACMD%" %MAVEN_OPTS% %MAVEN_DEBUG_OPTS% -classpath "%WRAPPER_JAR%" "-Dmaven.multiModuleProjectDirectory=%BASE_DIR%" org.apache.maven.wrapper.MavenWrapperMain %*
SET "ERROR_CODE=%ERRORLEVEL%"
ENDLOCAL & EXIT /B %ERROR_CODE%

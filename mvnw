#!/bin/sh
set -eu

BASE_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
WRAPPER_DIR="$BASE_DIR/.mvn/wrapper"
WRAPPER_JAR="$WRAPPER_DIR/maven-wrapper.jar"
WRAPPER_PROPERTIES="$WRAPPER_DIR/maven-wrapper.properties"

if [ -n "${JAVA_HOME:-}" ]; then
  JAVACMD="$JAVA_HOME/bin/java"
else
  JAVACMD="$(command -v java || true)"
fi

if [ -z "${JAVACMD:-}" ] || [ ! -x "$JAVACMD" ]; then
  echo "Error: Java executable not found. Set JAVA_HOME or add java to PATH." >&2
  exit 1
fi

if [ ! -r "$WRAPPER_JAR" ]; then
  if [ ! -f "$WRAPPER_PROPERTIES" ]; then
    echo "Error: Missing $WRAPPER_PROPERTIES" >&2
    exit 1
  fi

  DOWNLOAD_URL="$(grep '^wrapperUrl=' "$WRAPPER_PROPERTIES" | cut -d'=' -f2-)"
  if [ -z "${DOWNLOAD_URL:-}" ]; then
    DOWNLOAD_URL="https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.2/maven-wrapper-3.3.2.jar"
  fi

  mkdir -p "$WRAPPER_DIR"
  echo "Downloading Maven wrapper from $DOWNLOAD_URL"

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "$WRAPPER_JAR" "$DOWNLOAD_URL"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$WRAPPER_JAR" "$DOWNLOAD_URL"
  else
    echo "Error: Neither curl nor wget is available to download $WRAPPER_JAR" >&2
    exit 1
  fi
fi

exec "$JAVACMD" \
  ${MAVEN_OPTS:-} \
  ${MAVEN_DEBUG_OPTS:-} \
  -classpath "$WRAPPER_JAR" \
  "-Dmaven.multiModuleProjectDirectory=$BASE_DIR" \
  org.apache.maven.wrapper.MavenWrapperMain "$@"

@ECHO OFF
echo "MyCluster installer"

pushd ..

echo "Checking for Python
REM Check for python 2.6 or 2.7
FOR /F "tokens=1,2" %%G IN ('"python.exe -V 2>&1"') DO ECHO %%H | find "2.6" > Nul
IF NOT ErrorLevel 1 GOTO PythonOK 
FOR /F "tokens=1,2" %%G IN ('"python.exe -V 2>&1"') DO ECHO %%H | find "2.7" > Nul
IF NOT ErrorLevel 1 GOTO PythonOK 
ECHO Requires Python 2.6 or 2.7
GOTO EOF

:PythonOK 
echo "Checking for virtualenv"

REM Check for virtualenv
WHERE virtualenv --version 2>NUL
IF %ERRORLEVEL% == 0 GOTO VirtualEnvOK
ECHO virtualenv not found
GOTO EOF
:VirtualEnvOK

if exist ".\mycluster-env\" rd /q /s ".\mycluster-env\"

echo "Creating virtual environment"
virtualenv mycluster-env

echo "Activating virtual environment"
call mycluster-env\scripts\activate

echo "Installing yolk"
pip install yolk

echo "Installing requirements"
pip install -r requirements.txt 

echo "Installing MyCluster"
pip install MyCluster

yolk -l

popd

:EOF

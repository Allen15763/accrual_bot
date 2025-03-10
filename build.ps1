# build.ps1

# 設置執行策略
Set-ExecutionPolicy Unrestricted -Scope Process -Force

# 激活虛擬環境
& .\venv\Scripts\activate

# 提示用戶輸入變數
$executableName = Read-Host 'Enter the name for the executable'

# 構建 bot
pyinstaller src\popr_ui.py --name $executableName -F -w --add-data "src\config.ini;."
Dim WinScriptHost
Set WinScriptHost = CreateObject("WScript.Shell")
WinScriptHost.Run Chr(34) & "E:\modetail\mo_report.bat" & Chr(34), 0
Set WinScriptHost = Nothing
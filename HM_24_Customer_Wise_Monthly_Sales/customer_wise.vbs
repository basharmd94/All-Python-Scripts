Dim WinScriptHost
Set WinScriptHost = CreateObject("WScript.Shell")
WinScriptHost.Run Chr(34) & "E:\zepto_customer_sale_ib\customer_wise.bat" & Chr(34), 0
Set WinScriptHost = Nothing
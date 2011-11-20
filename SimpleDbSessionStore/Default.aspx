<%@ Page Language="C#" AutoEventWireup="false" CodeBehind="Default.aspx.cs" Inherits="SimpleDbSessionStore.Default" %>

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
	<meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
	<meta charset="utf-8" />
    <title>SimpleDB Session Store</title>
</head>
<body>
    <div>
        <%: Session["counter"] %>
    </div>
</body>
</html>

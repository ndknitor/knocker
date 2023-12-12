using CommandLine;

//string[] sqlserver = ["-i", "./data", "-d", ",", "-e", "__EFMigrationsHistory", "-c", "Data Source=127.0.0.1;TrustServerCertificate=True;Initial Catalog=Etdb;User ID=sa;Password=12345678aA#"];
//string[] mysql = ["-i", "data", "-d", ",", "-p", "mysql", "-c", "Server=127.0.0.1;Database=Etdb;User Id=root;Password=12345678aA#"];
//string[] postgres = ["-i", "data", "-d", ",", "-p", "postgres", "-c", "Server=127.0.0.1;Database=Etdb;User Id=odoo;Password=odoo;"];

Parser.Default.ParseArguments<ResetDbOption>(args)
.WithParsed<ResetDbOption>(o => o.Call());

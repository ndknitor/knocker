using CommandLine;

//string[] arguments = ["reset", "-i", "./data", "-d", ",", "-e", "__EFMigrationsHistory", "-c", "Data Source=127.0.0.1;TrustServerCertificate=True;Initial Catalog=Etdb;User ID=sa;Password=12345678aA#"];
//string[] arguments = ["reset", "-i", "data", "-d", ",", "-p", "mysql", "-c", "Server=127.0.0.1;Database=Etdb;User Id=root;Password=12345678aA#"];
//string[] arguments = ["reset","-i", "data", "-d", ",", "-p", "postgres", "-e", "User", "-c", "Server=127.0.0.1;Database=Etdb;User Id=postgres;Password=12345678aA#;"];

//string[] arguments = ["delete", "-p", "mysql", "-c", "Server=127.0.0.1;Database=Test;User Id=root;Password=12345678aA#"];
//string[] arguments = ["delete","-p", "postgres", "-e", "User", "-c", "Server=127.0.0.1;Database=Etdb;User Id=odoo;Password=odoo;"];


Parser.Default.ParseArguments<ResetDbOption, DeleteDbOption>(args)
.WithParsed<ResetDbOption>(o => o.Call())
.WithParsed<DeleteDbOption>(o => o.Call());

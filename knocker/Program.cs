using CommandLine;
//string[] sqlserver = ["-i", "./data", "-d", ",", "-e", "__EFMigrationsHistory", "-c", "Data Source=127.0.0.1;TrustServerCertificate=True;Initial Catalog=Etdb;User ID=sa;Password=12345678aA#"];
//string[] mysql = ["-i", "data/Ticket.csv", "data/User.csv", "data/Route.csv", "data/Bus.csv", "data/Trip.csv", "data/Seat.csv",  "-d", ",",  "-p", "mysql", "-c", "Server=127.0.0.1;Database=Etdb;User Id=root;Password=12345678aA#;Allow User Variables=true"];
//string[] postgres = ["-i", "data/Ticket.csv", "data/User.csv", "data/Route.csv", "data/Bus.csv", "data/Trip.csv", "data/Seat.csv", "-d", ",", "-p", "postgres", "-c", "Server=127.0.0.1;Database=Etdb;User Id=odoo;Password=odoo;"];

Parser.Default.ParseArguments<ResetDbOption>(args)
.WithParsed<ResetDbOption>(o => o.Call());

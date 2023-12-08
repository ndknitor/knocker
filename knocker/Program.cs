using CommandLine;
//string[] sqlserver = ["-i", "User.csv", "Route.csv", "Bus.csv", "Trip.csv", "Seat.csv", "Ticket.csv", "-d", ",", "-e", "__EFMigrationsHistory", "-c", "Data Source=127.0.0.1;TrustServerCertificate=True;Initial Catalog=Etdb;User ID=sa;Password=12345678aA#"];
//string[] mysql = ["-i", "User.csv", "Route.csv", "Bus.csv", "Trip.csv", "Seat.csv", "Ticket.csv", "-d", ",", "-e", "__EFMigrationsHistory", "-p", "mysql", "-c", "Server=127.0.0.1;Database=Etdb;User Id=root;Password=12345678aA#;Allow User Variables=true"];

Parser.Default.ParseArguments<ResetDbOption>(args)
.WithParsed<ResetDbOption>(o => o.Call());

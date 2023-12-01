using CommandLine;
string[] s = ["-i" ,"User.csv", "--connectionstring" ,"Data Source=127.0.0.1;TrustServerCertificate=True;Initial Catalog=Etdb;User ID=sa;Password=12345678aA#"];
await Parser.Default.ParseArguments<ResetDbOption>(s)
.WithParsedAsync<ResetDbOption>(async o => await o.Call());

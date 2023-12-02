using CommandLine;
string[] s = ["-i" ,"User.csv", "-d" , "," ,"-c" ,"Data Source=127.0.0.1;TrustServerCertificate=True;Initial Catalog=Etdb;User ID=sa;Password=12345678aA#"];
string[] mysql = ["-i" ,"User.csv", "-d" ,"," , "-p", "mysql" ,"-c" ,"Server=127.0.0.1;Database=Etdb;User Id=root;Password=12345678aA#;Allow User Variables=true"];

await Parser.Default.ParseArguments<ResetDbOption>(mysql)
.WithParsedAsync<ResetDbOption>(async o => await o.Call());

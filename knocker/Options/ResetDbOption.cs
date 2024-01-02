using System.Data.SqlClient;
using CommandLine;
using MySqlConnector;

[Verb("reset", HelpText = "Reset database from CSV files")]
public class ResetDbOption
{
    [Option('i', "input", Required = true, HelpText = "Set data input to reset path (CSV)")]
    public string InputPath { get; set; }
    [Option('c', "connectionstring", Required = true, HelpText = "Connection string that connect to the database")]
    public string ConnectionString { get; set; }
    [Option('d', "delimiter", Required = false, HelpText = "Delimter of the CSV file. Default: ';'")]
    public string Delimiter { get; set; } = ";";
    [Option('p', "provider", Required = false, HelpText = "Database provider (mssql, mysql, postgres). Default: mssql")]
    public string Provider { get; set; } = "mssql";
    [Option('e', "exclude", Required = false, HelpText = "Exclude tables")]
    public IEnumerable<string> ExcludeTables { get; set; } = new List<string>();
    [Option('r', "reset", Required = false, HelpText = "Reset auto-increament columns")]
    public bool ResetAutoIncreasement { get; set; } = true;
    public void Call()
    {
        string[] allowedProviders = { Const.mssql, Const.mysql, Const.postgres };
        if (Array.IndexOf(allowedProviders, Provider) == -1)
        {
            Console.WriteLine($"Invalid database provider: {Provider}. Allowed values are: {string.Join(", ", allowedProviders)}.");
            Environment.Exit(1);
        }
        try
        {
            DirectoryInfo directory = new DirectoryInfo(InputPath);
            var files = directory.GetFiles().Where(f => f.Extension == ".csv").Select(f => f.FullName);

            IService service = null;
            switch (Provider)
            {
                default: service = new MssqlService { Connection = new SqlConnection(ConnectionString), Delimiter = Delimiter, ExcludeTables = ExcludeTables, InputPaths = files, ResetAutoIncreasement = ResetAutoIncreasement }; break;
                case Const.mssql: service = new MssqlService { Connection = new SqlConnection(ConnectionString), Delimiter = Delimiter, ExcludeTables = ExcludeTables, InputPaths = files, ResetAutoIncreasement = ResetAutoIncreasement }; break;
                case Const.mysql: service = new MysqlService { Connection = new MySqlConnection(ConnectionString), Delimiter = Delimiter, ExcludeTables = ExcludeTables, InputPaths = files, ResetAutoIncreasement = ResetAutoIncreasement }; break;
                case Const.postgres: service = new PostgresService { Connection = new Npgsql.NpgsqlConnection(ConnectionString), Delimiter = Delimiter, ExcludeTables = ExcludeTables, InputPaths = files, ResetAutoIncreasement = ResetAutoIncreasement }; break;
            }
            service.PerformReset();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            Environment.Exit(1);
        }

    }
}
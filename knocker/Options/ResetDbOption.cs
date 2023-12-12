using System.Data.SqlClient;
using CommandLine;
using MySqlConnector;

[Verb("reset", HelpText = "Reset database from file")]
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
    public void Call()
    {
        string[] allowedProviders = { mssql, mysql, postgres };
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
                default: service = new MssqlService(new SqlConnection(ConnectionString), files, Delimiter, ExcludeTables); break;
                case mssql: service = new MssqlService(new SqlConnection(ConnectionString), files, Delimiter, ExcludeTables); break;
                case mysql: service = new MysqlService(new MySqlConnection(ConnectionString), files, Delimiter, ExcludeTables); break;
                case postgres: service = new PostgresService(new Npgsql.NpgsqlConnection(ConnectionString), files, Delimiter, ExcludeTables); break;
            }
            service.PerformReset();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            Environment.Exit(1);
            throw;
        }

    }
    const string mssql = "mssql";
    const string mysql = "mysql";
    const string postgres = "postgres";
}
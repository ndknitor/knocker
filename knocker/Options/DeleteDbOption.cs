using System.Data.SqlClient;
using CommandLine;
using MySqlConnector;

[Verb("delete", HelpText = "Delete all data in database")]
public class DeleteDbOption
{
    [Option('c', "connectionstring", Required = true, HelpText = "Connection string that connect to the database")]
    public string ConnectionString { get; set; }
    [Option('p', "provider", Required = false, HelpText = "Database provider (mssql, mysql, postgres). Default: mssql")]
    public string Provider { get; set; } = Const.mssql;
    [Option('e', "exclude", Required = false, HelpText = "Exclude tables")]
    public IEnumerable<string> ExcludeTables { get; set; } = new List<string>();
    public void Call()
    {
        string[] allowedProviders = { Const.mssql, Const.mysql, Const.postgres };
        if (Array.IndexOf(allowedProviders, Provider) == -1)
        {
            Console.WriteLine(
$@"Invalid database provider: {Provider}.
Allowed values are: {string.Join(", ", allowedProviders)}.");
            Environment.Exit(1);
        }
        try
        {
            IService service = null;
            switch (Provider)
            {
                default: service = new MssqlService { Connection = new SqlConnection(ConnectionString), ExcludeTables = ExcludeTables }; break;
                case Const.mssql: service = new MssqlService { Connection = new SqlConnection(ConnectionString), ExcludeTables = ExcludeTables }; break;
                case Const.mysql: service = new MysqlService { Connection = new MySqlConnection(ConnectionString), ExcludeTables = ExcludeTables }; break;
                case Const.postgres: service = new PostgresService { Connection = new Npgsql.NpgsqlConnection(ConnectionString), ExcludeTables = ExcludeTables }; break;
            }
            service.PerformDelete();
            Console.WriteLine("Delete all data successfully");
        }
        catch (System.Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            Environment.Exit(1);
            throw;
        }
    }
}
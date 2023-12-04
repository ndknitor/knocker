using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using CommandLine;
using CsvHelper;
using Dapper;
using MySqlConnector;

[Verb("resetdb", HelpText = "Reset database from file")]
public class ResetDbOption
{
    [Option('i', "input", Required = true, HelpText = "Set data input to reset path (CSV)")]
    public IEnumerable<string> InputPaths { get; set; }
    [Option('c', "connectionstring", Required = true, HelpText = "Connection string that connect to the database")]
    public string ConnectionString { get; set; }
    [Option('d', "delimiter", Required = false, HelpText = "Delimter of the CSV file")]
    public string Delimiter { get; set; } = ";";
    [Option('p', "provider", Required = false, HelpText = "Database provider")]
    public string Provider { get; set; } = "mssql";
    private IDbConnection c;
    private IDbConnection connection
    {
        get
        {
            if (c == null)
            {
                switch (Provider)
                {
                    default: c = new SqlConnection(ConnectionString); break;
                    case mssql: c = new SqlConnection(ConnectionString); break;
                    case mysql: c = new MySqlConnection(ConnectionString); break;
                }
            }
            return c;
        }
    }

    public async Task Call()
    {
        string[] allowedProviders = { mssql, mysql, postgres };
        if (Array.IndexOf(allowedProviders, Provider) == -1)
        {
            Console.WriteLine($"Invalid database provider: {Provider}. Allowed values are: {string.Join(", ", allowedProviders)}.");
            Environment.Exit(1);
        }
        try
        {
            await DeleteData();
            await InsertData();
        }
        catch (System.Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            Environment.Exit(1);
            throw;
        }

    }
    private async Task DeleteData()
    {
        switch (Provider)
        {
            default: await MssqlDeleteData(); break;
            case mssql: await MssqlDeleteData(); break;
            case mysql: await MysqlDeleteData(); break;
            case postgres: Console.Error.WriteLine("Future feature, postgres is not yet supported."); Environment.Exit(1); break;
        }
    }
    private async Task PostgresDeleteData()
    {

    }
    private async Task InsertData()
    {
        foreach (string csvFilePath in InputPaths)
        {
            var tableName = new FileInfo(csvFilePath).Name.Split('.')[0];
            string[] columns = null;
            IEnumerable<IDictionary<string, object>> data = ReadCsv(csvFilePath);
            if (data.Count() > 0)
            {
                columns = data.ElementAt(0).Keys.ToArray();
            }
            else
            {
                Console.WriteLine($"No data include for table {tableName}");
                continue;
            }
            string insertCommand = null;
            string columnsText = string.Join(", ", columns);
            switch (Provider)
            {
                default: insertCommand = $"INSERT INTO [{tableName}] ({columnsText}) VALUES (@{string.Join(", @", data.ElementAt(0).Keys)})"; break;
                case mssql: insertCommand = $"INSERT INTO [{tableName}] ({columnsText}) VALUES (@{string.Join(", @", data.ElementAt(0).Keys)})"; break;
                case mysql: insertCommand = $"INSERT INTO {tableName} ({columnsText}) VALUES (@{string.Join(", @", data.ElementAt(0).Keys)})"; break;
            }
            await connection.ExecuteAsync(insertCommand, data);
            Console.WriteLine($"Data reseted into table {tableName} successfully.");
        }
    }
    private async Task MssqlDeleteData()
    {
        await connection.ExecuteAsync(@"
EXEC sp_MSforeachtable 'DISABLE TRIGGER ALL ON ?'
EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'
EXEC sp_MSforeachtable 'SET QUOTED_IDENTIFIER ON; DELETE FROM ?'
EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'
EXEC sp_MSforeachtable 'ENABLE TRIGGER ALL ON ?'");
    }
    private async Task MysqlDeleteData()
    {
        string deleteQuery = await connection.QueryFirstAsync<string>(@"
SET @tables = NULL;
SELECT GROUP_CONCAT(CONCAT('DELETE FROM ', table_name) SEPARATOR ';') INTO @tables
FROM information_schema.tables WHERE table_schema = @dbname;
SELECT @tables;", new { dbname = connection.Database });
        await connection.ExecuteAsync($"SET FOREIGN_KEY_CHECKS = 0;{deleteQuery};SET FOREIGN_KEY_CHECKS = 1;");
    }
    private IEnumerable<IDictionary<string, object>> ReadCsv(string filePath)
    {
        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = Delimiter
        }))
        {
            var records = csv.GetRecords<dynamic>();
            var result = new List<IDictionary<string, object>>();

            foreach (var record in records)
            {
                var dictionary = new Dictionary<string, object>();

                foreach (var keyValuePair in record)
                {
                    dictionary[keyValuePair.Key] = keyValuePair.Value;
                }

                result.Add(dictionary);
            }

            return result;
        }
    }
    const string mssql = "mssql";
    const string mysql = "mysql";
    const string postgres = "postgres";
}
public enum DatabaseProvider
{
    Mssql,
    Mysql,
    Postgres,
    SqlLite,
    MongoDb
}
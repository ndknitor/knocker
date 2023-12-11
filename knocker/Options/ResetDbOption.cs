using System.Data;
using System.Data.SqlClient;
using System.Text;
using CommandLine;
using KnCsvReader;
using Dapper;
using MySqlConnector;

[Verb("reset", HelpText = "Reset database from file")]
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
    [Option('e', "exclude", Required = false, HelpText = "Exclude tables")]
    public IEnumerable<string> ExcludeTables { get; set; } = new List<string>();
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
            DeleteData();
            InsertData();
        }
        catch (System.Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            Environment.Exit(1);
            throw;
        }

    }
    private void DeleteData()
    {
        switch (Provider)
        {
            default: MssqlDeleteData(); break;
            case mssql: MssqlDeleteData(); break;
            case mysql: MysqlDeleteData(); break;
            case postgres: PostgresDeleteData(); break;
        }
    }
    private void PostgresDeleteData()
    {
        Console.Error.WriteLine("Future feature, postgres is not yet supported.");
        Environment.Exit(1);
    }
    private void InsertData()
    {
        foreach (string csvFilePath in InputPaths)
        {
            var tableName = new FileInfo(csvFilePath).Name.Split('.')[0];
            string[] columns = null;
            IEnumerable<IDictionary<string, object>> data = Csv.ReadFile(csvFilePath, Delimiter);
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
            switch (Provider)
            {
                default: insertCommand = MssqlInsertCommand(columns, tableName); break;
                case mssql: insertCommand = MssqlInsertCommand(columns, tableName); break;
                case mysql: insertCommand = MysqlInsertCommand(columns, tableName); break;
            }
            connection.Execute(insertCommand, data);
            Console.WriteLine($"Table {tableName}'s data has been reseted successfully.");
        }
    }
    private string MssqlInsertCommand(string[] columns, string tableName)
    {
        StringBuilder columnsText = new StringBuilder();
        foreach (var item in columns)
        {
            columnsText.Append($"[{item}],");
        }
        columnsText.Remove(columnsText.Length - 1, 1);
        return $"INSERT INTO [{tableName}] ({columnsText}) VALUES (@{string.Join(", @", columns)})";
    }
    private string MysqlInsertCommand(string[] columns, string tableName)
    {
        StringBuilder columnsText = new StringBuilder();
        foreach (var item in columns)
        {
            columnsText.Append($"`{item}`,");
        }
        columnsText.Remove(columnsText.Length - 1, 1);
        return $"INSERT INTO {tableName} ({columnsText}) VALUES (@{string.Join(", @", columns)})";
    }
    private void MssqlDeleteData()
    {
        IEnumerable<string> tables = connection.Query<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'dbo' AND TABLE_NAME not in @excludeTables;", new { excludeTables = ExcludeTables });
        StringBuilder deleteQuery = new StringBuilder();
        foreach (string item in tables)
        {
            deleteQuery.AppendLine($"Delete from [{item}]");
        }
        connection.Execute($@"
        EXEC sp_MSForEachTable 'DISABLE TRIGGER ALL ON ?'
        EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'
        {deleteQuery}
        EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CONSTRAINT ALL'
        EXEC sp_MSForEachTable 'ENABLE TRIGGER ALL ON ?'");
    }
    private void MysqlDeleteData()
    {
        string deleteQuery = connection.QueryFirst<string>(@"
SET @tables = NULL;
SELECT GROUP_CONCAT(CONCAT('DELETE FROM ', table_name) SEPARATOR ';') INTO @tables
FROM information_schema.tables WHERE table_schema = @dbname AND TABLE_NAME not in @excludeTables;
SELECT @tables;", new { dbname = connection.Database, excludeTables = ExcludeTables });
        connection.Execute($"SET FOREIGN_KEY_CHECKS = 0;{deleteQuery};SET FOREIGN_KEY_CHECKS = 1;");
    }
    // private IEnumerable<IDictionary<string, object>> ReadCsv(string filePath)
    // {
    //     using (var reader = new StreamReader(filePath))
    //     using (var csv = new CsvReader(reader, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
    //     {
    //         Delimiter = Delimiter
    //     }))
    //     {
    //         var records = csv.GetRecords<dynamic>();
    //         var result = new List<IDictionary<string, object>>();

    //         foreach (var record in records)
    //         {
    //             var dictionary = new Dictionary<string, object>();

    //             foreach (var keyValuePair in record)
    //             {
    //                 string value = keyValuePair.Value;
    //                 int intValue = 0;
    //                 if (int.TryParse(value, out intValue))
    //                 {
    //                     dictionary[keyValuePair.Key] = intValue;
    //                 }
    //                 else
    //                 {
    //                     dictionary[keyValuePair.Key] = keyValuePair.Value;
    //                 }
    //             }

    //             result.Add(dictionary);
    //         }

    //         return result;
    //     }
    // }
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
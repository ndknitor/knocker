using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using CommandLine;
using CsvHelper;
using CsvHelper.Configuration;
using Dapper;
using Newtonsoft.Json;

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
    public DatabaseProvider Provider { get; set; }
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
                    case DatabaseProvider.Mssql: c = new SqlConnection(ConnectionString); break;
                }
            }
            return c;
        }
    }

    public async Task Call()
    {
        await DeleteData();
        await InsertData();
    }
    private async Task DeleteData()
    {
        try
        {
            switch (Provider)
            {
                default: await MssqlDeleteData(); break;
                case DatabaseProvider.Mysql: await MssqlDeleteData(); break;
            }
            Console.WriteLine("All data deleted successfully.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            Environment.Exit(1);
        }
    }
    private async Task InsertData()
    {
        try
        {
            foreach (string csvFilePath in InputPaths)
            {
                var tableName = new FileInfo(csvFilePath).Name.Split('.')[0];
                string[] columns = null;
                IEnumerable<dynamic> csvData = ReadCsv(csvFilePath, out columns);
                string columnsText = string.Join(", ", columns);
                //string insertCommand = $"INSERT INTO {tableName} ({columns}) VALUES (@{string.Join(", @", csvData[0].Keys)})";
                //await connection.ExecuteAsync(insertCommand, csvData);
                Console.WriteLine($"Data inserted into table {tableName} successfully.");
            }
        }
        catch (System.Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            Environment.Exit(1);
        }

    }

    private async Task MssqlDeleteData()
    {
        await connection.ExecuteAsync("EXEC sp_MSForEachTable 'DELETE FROM ?'");
    }

    public IEnumerable<dynamic> ReadCsv(string filePath, out string[] columns)
    {
        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = Delimiter }))
        {
            columns = csv.Context.Reader.HeaderRecord;
            return csv.GetRecords<dynamic>().ToList();
        }
    }
}
public enum DatabaseProvider
{
    Mssql,
    Mysql,
    Postgres,
    SqlLite,
    MongoDb
}
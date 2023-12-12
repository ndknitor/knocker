using System.Data;
using System.Text;
using Dapper.Transaction;
using KnCsvReader;
using Npgsql;

public class PostgresService : IService
{
    private readonly NpgsqlConnection connection;
    IEnumerable<string> inputPaths;
    string delimiter;
    IEnumerable<string> excludeTables;
    public PostgresService(NpgsqlConnection connection, IEnumerable<string> inputPaths, string delimiter, IEnumerable<string> excludeTables)
    {
        this.connection = connection;
        this.inputPaths = inputPaths;
        this.delimiter = delimiter;
        this.excludeTables = excludeTables;
    }

    public void PerformReset()
    {
        if (connection.State == ConnectionState.Closed)
            connection.Open();
        using (IDbTransaction transaction = connection.BeginTransaction())
        {
            try
            {
                transaction.Execute("SET session_replication_role = 'replica';");
                DeleteData(transaction);
                InsertData(transaction);
                transaction.Execute("SET session_replication_role = 'origin';");
                transaction.Commit();
            }
            catch (System.Exception)
            {
                transaction.Rollback();
                throw;
            }
        }
    }
    private void DeleteData(IDbTransaction transaction)
    {
        transaction.Execute(@"
        DO $$ DECLARE
            table_name TEXT;
        BEGIN
            FOR table_name IN (SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ) LOOP
                EXECUTE 'TRUNCATE TABLE ' || quote_ident(table_name) || ' CASCADE;';
            END LOOP;
        END $$;
        ", new { excludeTables = excludeTables });
    }
    private void InsertData(IDbTransaction transaction)
    {
        foreach (string csvFilePath in inputPaths)
        {
            var tableName = new FileInfo(csvFilePath).Name.Split('.')[0];
            string[] columns = null;
            IEnumerable<IDictionary<string, object>> data = Csv.ReadFile(csvFilePath, delimiter);
            if (data.Count() > 0)
            {
                columns = data.ElementAt(0).Keys.ToArray();
                transaction.Execute(InsertCommand(columns, tableName), data);
                Console.WriteLine($"Table {tableName}'s data has been reseted successfully.");
            }
            else
            {
                Console.WriteLine($"No data include for table {tableName}");
                continue;
            }
        }
    }
    private string InsertCommand(string[] columns, string tableName)
    {
        StringBuilder columnsText = new StringBuilder();
        foreach (var item in columns)
        {
            columnsText.Append($"\"{item}\",");
        }
        columnsText.Remove(columnsText.Length - 1, 1);
        return $"INSERT INTO \"{tableName}\" ({columnsText}) VALUES (@{string.Join(", @", columns)})";
    }
}
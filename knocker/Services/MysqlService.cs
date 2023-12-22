using System.Data;
using System.Text;
using Dapper.Transaction;
using KnCsvReader;
using MySqlConnector;

public class MysqlService : IService
{
    public MySqlConnection Connection { get; set; }
    public IEnumerable<string> InputPaths { get; set; }
    public string Delimiter { get; set; }
    public IEnumerable<string> ExcludeTables { get; set; }
    private IEnumerable<string> tables = null;
    public void PerformReset()
    {
        if (Connection.State == ConnectionState.Closed)
            Connection.Open();
        using (IDbTransaction transaction = Connection.BeginTransaction())
        {
            try
            {
                DisableForeignKeyCheck(transaction);
                DeleteData(transaction);
                ResetIdentityInsert(transaction);
                InsertData(transaction);
                EnableForeignKeyCheck(transaction);
                transaction.Commit();
            }
            catch (System.Exception)
            {
                transaction.Rollback();
                throw;
            }
        }
    }
    public void PerformDelete()
    {
        if (Connection.State == ConnectionState.Closed)
            Connection.Open();
        using (IDbTransaction transaction = Connection.BeginTransaction())
        {
            try
            {
                DisableForeignKeyCheck(transaction);
                DeleteData(transaction);
                ResetIdentityInsert(transaction);
                EnableForeignKeyCheck(transaction);
                transaction.Commit();
            }
            catch (System.Exception)
            {
                transaction.Rollback();
                throw;
            }
        }
    }
    private void DisableForeignKeyCheck(IDbTransaction transaction)
    {
        transaction.Execute("SET FOREIGN_KEY_CHECKS = 0;");
    }
    private void EnableForeignKeyCheck(IDbTransaction transaction)
    {
        transaction.Execute("SET FOREIGN_KEY_CHECKS = 1;");
    }
    private void DeleteData(IDbTransaction transaction)
    {
        tables ??= GetTables(transaction);
        StringBuilder deleteQuery = new StringBuilder();
        foreach (string item in tables)
        {
            deleteQuery.AppendLine($"Delete from `{item}`;");
        }
        transaction.Execute(deleteQuery.ToString());
    }
    private void InsertData(IDbTransaction transaction)
    {
        foreach (string csvFilePath in InputPaths)
        {
            var tableName = new FileInfo(csvFilePath).Name.Split('.')[0];
            string[] columns = null;
            IEnumerable<IDictionary<string, object>> data = Csv.ReadFile(csvFilePath, Delimiter);
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
            columnsText.Append($"`{item}`,");
        }
        columnsText.Remove(columnsText.Length - 1, 1);
        return $"INSERT INTO {tableName} ({columnsText}) VALUES (@{string.Join(", @", columns)})";
    }
    private void ResetIdentityInsert(IDbTransaction transaction)
    {
        tables ??= GetTables(transaction);
        StringBuilder query = new StringBuilder();
        foreach (var table in tables)
        {
            if (HaveIdentityColumn(table, transaction))
            {
                query.AppendLine($"ALTER TABLE {table} AUTO_INCREMENT = 1;");
            }
        }
        if (query.Length > 0)
        {
            transaction.Execute(query.ToString());
        }
    }
    private bool HaveIdentityColumn(string tableName, IDbTransaction transaction) => transaction.QueryFirstOrDefault<bool>("SELECT Count(*) COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = @tableName AND EXTRA = 'auto_increment'", new { tableName });
    private IEnumerable<string> GetTables(IDbTransaction transaction) => transaction.Query<string>("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = @dbname AND TABLE_NAME not in @excludeTables;", new { excludeTables = ExcludeTables, dbname = transaction.Connection.Database });

}
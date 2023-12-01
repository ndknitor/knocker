using CommandLine;
using Newtonsoft.Json.Linq;

[Verb("j2c", HelpText = "Json file to Csv file")]
public class JsonToCsvOption
{
    [Option('o', "input", Required = true, HelpText = "Set Csv output path")]
    public string InputPath { get; set; }
    [Option('d', "delimiter", Required = false, HelpText = "Csv delimiter character")]
    public char Delimiter { get; set; } = ';';
    [Option('o', "output", Required = false, HelpText = "Set Csv output path")]
    public string OutputPath { get; set; }
    public void Call()
    {
        string outputPath = OutputPath ?? Path.Combine(Environment.CurrentDirectory, Path.GetFileNameWithoutExtension(InputPath) + ".csv");
        Console.WriteLine($"Converting {InputPath} to CSV and saving to {outputPath}");
        try
        {
            // Read JSON from the input file
            string jsonContent = File.ReadAllText(InputPath);
            JArray jsonArray = JArray.Parse(jsonContent);

            // Convert JSON array to CSV
            string csvContent = ConvertJsonToCsv(jsonArray, Delimiter);

            // Write CSV to the output file
            File.WriteAllText(outputPath, csvContent);

            Console.WriteLine("Conversion complete!");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            //Console.WriteLine($"Error: {ex.Message}");
        }
    }

    private string ConvertJsonToCsv(JArray jsonArray, char delimiter)
    {
        StringWriter csvString = new StringWriter();

        // Write header
        JToken firstObject = jsonArray.First;
        foreach (JProperty property in firstObject.Children<JProperty>())
        {
            csvString.Write(property.Name);
            csvString.Write(delimiter);
        }
        csvString.WriteLine();

        // Write data
        foreach (JObject obj in jsonArray)
        {
            foreach (JProperty property in obj.Children<JProperty>())
            {
                csvString.Write(property.Value);
                csvString.Write(delimiter);
            }
            csvString.WriteLine();
        }

        return csvString.ToString();
    }
}
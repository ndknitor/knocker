using CommandLine;

[Verb("c2j", HelpText = "Csv file to Json file")]
public class CsvToJsonOption
{
    [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
    public bool Verbose { get; set; }
    public void Call()
    {

    }
}
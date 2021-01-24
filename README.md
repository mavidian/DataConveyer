# DataConveyer

Data&nbsp;Conveyer is toolkit and a lightweight transformation engine to facilitate real-time data migrations.
It enables rapid implementation of data centric solutions and can apply to any .NET project targeting .NET Framework 4.5 or .NET Standard 2.0.

Data&nbsp;Conveyer can be added to a .NET project by simply installing its [NuGet package](https://www.nuget.org/packages/DataConveyer/).

Details on Data&nbsp;Conveyer features, usage samples, self-guided tutorial, etc. are available at [Data&nbsp;Conveyer site](http://www.dataconveyer.com).

A complete reference guide of the API exposed by Data&nbsp;Conveyer is available at [Data&nbsp;Conveyer Help portal][help_ref].

## Installation

In order to add Data&nbsp;Conveyer to your .NET project, it is recommended use the "Manage NuGet Packages..." option in Visual Studio,
(available upon right-clicking on the project in the Solution Explorer window).

Data&nbsp;Conveyer NuGet package can also be added using Package Manager:

```powershell
Install-Package DataConveyer -Version n.n.n
```

or .NET CLI:

```dotnetcli
dotnet add package DataConveyer --version n.n.n
```

, where n.n.n represents the version number (e.g. 3.0.1).

Alternatively, if you prefer to build the Data&nbsp;Conveyer yourself, you can fork this repository and clone it onto your local machine.

## Usage

Once Data&nbsp;Conveyer is added to your project, its use typically involves these 3 steps:

* Configure Data&nbsp;Conveyer's orchestrator by defining input/output data types, transformation functions, etc.
* Launch Data&nbsp;Conveyer's process by calling the `ExecuteAsync` method.
* Evaluate `ProcessResult` returned by Data&nbsp;Conveyer.

The following code snippet represents a simple Data&nbsp;Conveyer's usage:

```csharp
using Mavidian.DataConveyer;
using Mavidian.DataConveyer.Orchestrators;
var config = new OrchestratorConfig()
{
  InputDataKind = KindOfTextData.Delimited,
  InputFileName = "input.csv",
  HeadersInFirstInputRow = true,
  OutputDataKind = KindOfTextData.Keyword,
  OutputFileName = "output.kw"
};
ProcessResult rslt;
using (var orchtr = OrchestratorCreator.GetEtlOrchestrator(config))
{
  rslt = orchtr.ExecuteAsync().Result;
}
if (rslt.CompletionStatus == CompletionStatus.IntakeDepleted)
{
  Console.WriteLine($"Successfully converted {rslt.RowsWritten} records!");
}
```

For additional examples on using Data&nbsp;Conveyer, please examine the [Data&nbsp;Conveyer tutorial](http://www.mavidian.com/dataconveyer/tutorial/) or the Data&nbsp;Conveyer Primer section of the [Data&nbsp;Conveyer Help portal][help_ref].

## Notes on the solution structure

The DataConveyer solution consist of the following projects:

* **DataConveyer** &nbsp;A class library that implements all Data&nbsp;Conveyer's functionality.
* **DataConveyer.Tests** &nbsp;Tests using xUnit.NET framework.
* **DataConveyer_doc** &nbsp; [Sandcastle](https://github.com/EWSoftware/SHFB) documentation project (its output is [Data&nbsp;Conveyer Help portal][help_ref].
* **DataConveyer_tests** &nbsp;Tests using MSTest framework.
* **TextDataHandler** &nbsp;A simple console application that executes sample Data&nbsp;Conveyer's processes.

You will notice 2 separate test projects. This is for historical reasons. Initial tests were developed using the MSTest framework. Due to the need for additional functionality (e.g. test repeats), some of the tests were subsequently ported to xUnit.NET framework. The idea is that at some point in the future only xUnit.NET tests will remain, so that the DataConveyer_tests project can be removed.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)

## Copyright

```
Copyright © 2019-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
```

[help_ref]: https://mavidian.github.io/DataConveyer-help/

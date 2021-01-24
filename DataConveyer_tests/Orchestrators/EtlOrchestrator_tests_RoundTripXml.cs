//EtlOrchestrator_tests_RoundTripXml.cs
//
// Copyright © 2018-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DataConveyer_tests.Orchestrators
{
   [TestClass]
   public class EtlOrchestrator_tests_RoundTripXml
   {
      //These are integration tests that involve both XML parsing and XML writing.
      //The idea is that for "normalized" XML (no "fancy" stuff like redundant matching nodes, attributes
      //in pattern for record match (which would become redundant items on output) or standardized formating),
      //the input data upon being parsed and then written to using the same settings should result in the same data on output.

      //The code in this class is practcally identical to EtlOrchestrator_tests_RoundTripXml.
      //The main reason for two distinct test clasess is to keep XML and JSON tests separate.   

      // Test data is kept in this dictionary:
      private Dictionary<string,            // testCase, i.e. key (e.g. Members_1_
                         Tuple<string,      // XML text (to be parsed at intake and then compared against on output)
                               string       // XML settings (for both intake and output)
                              >
                        > _testDataRepo;

      OrchestratorConfig _config;

      //Test results:
      TextWriter _outputWriter;
      StringBuilder _outputData;

      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<string, string>>();
         _config = new OrchestratorConfig();

         _outputData = new StringBuilder();
         _outputWriter = new StringWriter(_outputData);


#region Test_1:
         //simple collection node
         _testDataRepo.Add("Test_1", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""utf-16""?>
<Members>
  <Member>
    <ID>1</ID>
    <FName>Paul</FName>
    <LName>Smith</LName>
    <DOB>1/12/1988</DOB>
  </Member>
  <Member>
    <ID>2</ID>
    <FName>John</FName>
    <LName>Green</LName>
    <DOB>8/23/1967</DOB>
  </Member>
  <Member>
    <ID>3</ID>
    <FName>Joseph</FName>
    <LName>Doe</LName>
    <DOB>11/6/1994</DOB>
  </Member>
</Members>",
             //settings:
             "CollectionNode|Members,RecordNode|Member,IndentChars|  "  //IndentChars relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_1
#endregion Test_1


#region Test_2:
         //simple observe clusters
         _testDataRepo.Add("Test_2", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""utf-16""?>
<Members>
  <Family>
    <Member>
      <ID>1</ID>
      <FName>Paul</FName>
      <LName>Smith</LName>
      <DOB>1/12/1988</DOB>
    </Member>
  </Family>
  <Family>
    <Member>
      <ID>2</ID>
      <FName>John</FName>
      <LName>Green</LName>
      <DOB>8/23/1967</DOB>
    </Member>
    <Member>
      <ID>4</ID>
      <FName>Johnny</FName>
      <LName>Green</LName>
      <DOB>5/3/1997</DOB>
    </Member>
  </Family>
  <Family>
    <Member>
      <ID>3</ID>
      <FName>Joseph</FName>
      <LName>Doe</LName>
      <DOB>11/6/1994</DOB>
    </Member>
  </Family>
</Members>",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member,IndentChars|  "  //IndentChars relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_2
#endregion Test_2


#region Test_2a:
         //observe clusters, attributes
         _testDataRepo.Add("Test_2a", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""utf-16""?>
<Members>
  <Family>
    <Member ID=""1"">
      <FName>Paul</FName>
      <LName>Smith</LName>
      <DOB>1/12/1988</DOB>
    </Member>
  </Family>
  <Family>
    <Member ID=""2"">
      <FName>John</FName>
      <LName>Green</LName>
      <DOB>8/23/1967</DOB>
    </Member>
    <Member ID=""4"">
      <FName>Johnny</FName>
      <LName>Green</LName>
      <DOB>5/3/1997</DOB>
    </Member>
  </Family>
  <Family>
    <Member ID=""3"">
      <FName>Joseph</FName>
      <LName>Doe</LName>
      <DOB>11/6/1994</DOB>
    </Member>
  </Family>
</Members>",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member,IncludeAttributes|truePlain,AttributeFields|ID,IndentChars|  "  //IncludeAttributes relevant only on intake; IndentChars and AttributeFields relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_2a
#endregion Test_2a


#region Test_3:
         //multi-level
         _testDataRepo.Add("Test_3", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""utf-16""?>
<Members>
  <X>
    <Family>
      <A>
        <X>
          <B>
            <Member>
              <ID>1</ID>
              <FName>Paul</FName>
              <LName>Smith</LName>
              <DOB>1/12/1988</DOB>
            </Member>
          </B>
        </X>
      </A>
    </Family>
  </X>
  <X>
    <Family>
      <A>
        <X>
          <B>
            <Member>
              <ID>2</ID>
              <FName>John</FName>
              <LName>Green</LName>
              <DOB>8/23/1967</DOB>
            </Member>
          </B>
        </X>
        <X>
          <B>
            <Member>
              <ID>4</ID>
              <FName>Johnny</FName>
              <LName>Green</LName>
              <DOB>5/3/1997</DOB>
            </Member>
          </B>
        </X>
      </A>
    </Family>
  </X>
  <X>
    <Family>
      <A>
        <X>
          <B>
            <Member>
              <ID>3</ID>
              <FName>Joseph</FName>
              <LName>Doe</LName>
              <DOB>11/6/1994</DOB>
            </Member>
          </B>
        </X>
      </A>
    </Family>
  </X>
</Members>",
             //settings:
             "CollectionNode|Members,ClusterNode|X/Family/A,RecordNode|X/B/Member,IndentChars|  "  //IndentChars relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_3
         #endregion Test_3


#region Test_3a:
         //multi-level with attributes
         //Note the record level pattern attribute  (such as RecordNode|Data/Member[@class=\"main\"]) cannot be used
         // as it would've resulted in redundant class="main" attributes on output (i.e. XML error).
         //Of course, workarounds exist, e.g. different intaake specs from output or remove class item during transformation
         _testDataRepo.Add("Test_3a", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Members region=""North"">
    <Group id=""2"" zone="""">
      <Subgroup>
        <Family>
          <Data>
            <Member class=""main"">
              <ID>1</ID>
              <FName>Paul</FName>
              <LName>Smith</LName>
              <Weight>180</Weight>
              <DOB>1/12/1988</DOB>
            </Member>
          </Data>
        </Family>
      </Subgroup>
    </Group>
    <Group id=""2"" zone="""">
      <Subgroup>
        <Family>
          <Data>
            <Member class=""main"">
              <ID>2</ID>
              <FName>John</FName>
              <LName>Green</LName>
              <DOB>8/23/1967</DOB>
            </Member>
          </Data>
          <Data>
            <Member class=""aux"">
              <ID>4</ID>
              <FName>Johnny</FName>
              <LName>Green</LName>
              <DOB>5/3/1997</DOB>
            </Member>
          </Data>
        </Family>
      </Subgroup>
    </Group>
    <Group id=""2"" zone="""">
      <Subgroup>
        <Family>
          <Data>
            <Member class=""other"">
              <ID>3</ID>
              <FName>Joseph</FName>
              <LName>Doe</LName>
              <Weight>195</Weight>
              <EyeColor>Brown</EyeColor>
              <DOB>11/6/1994</DOB>
            </Member>
          </Data>
        </Family>
      </Subgroup>
    </Group>
  </Members>
</Root>",
             //settings:
             "CollectionNode|Root/Members[@region=\"North\"],ClusterNode|Group[@id=2][@zone=\"\"]/Subgroup/Family,RecordNode|Data/Member,IncludeAttributes|truePlain,AttributeFields|class,IndentChars|  "  //IncludeAttributes relevant only on intake; IndentChars and AttributeFields relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_3a
#endregion Test_3a

      }  //Initialize


  [DataTestMethod]
      [DataRow("Test_1")]
      [DataRow("Test_2")]
      [DataRow("Test_2a")]
      [DataRow("Test_3")]
      [DataRow("Test_3a")]
      public void Xml_RoundTrip_CorrectData(string testCase)
      {
         //arrange
         var testData = _testDataRepo[testCase];
         var xmlText = testData.Item1;
         var settings = testData.Item2;
         var intakeReader = new StringReader(xmlText);

         _config.IntakeReader = () => intakeReader;
         _config.InputDataKind = KindOfTextData.XML;
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = settings;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.XML;
         _config.XmlJsonOutputSettings = settings;  //same as for intake
         _config.OutputWriter = () => _outputWriter;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         _outputData.ToString().Should().Be(xmlText);
      }
   }
}

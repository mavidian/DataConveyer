//EtlOrchestrator_tests_RoundTripJson.cs
//
// Copyright © 2018-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
   public class EtlOrchestrator_tests_RoundTripJson
   {
      //These are integration tests that involve both JSON parsing and JSON writing.
      //The idea is that for "normalized" JSON (no "fancy" stuff like no array for single objects
      //and standardized formating), the input data upon being parsed and then written to using
      //the same settings should result in the same data on output.

      //The code in this class is practically identical to EtlOrchestrator_tests_RoundTripXml.
      //The main reason for two distinct test clasess is to keep XML and JSON tests separate.   

      // Test data is kept in this dictionary:
      private Dictionary<string,            // testCase, i.e. key (e.g. Members_1_
                         Tuple<string,      // JSON text (to be parsed at intake and then compared against on output)
                               string       // JSON settings (for both intake and output)
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
//inputJSON:
@"{
  ""Members"": {
    ""Member"": [
      {
        ""ID"": ""1"",
        ""FName"": ""Paul"",
        ""LName"": ""Smith"",
        ""DOB"": ""1/12/1988""
      },
      {
        ""ID"": ""2"",
        ""FName"": ""John"",
        ""LName"": ""Green"",
        ""DOB"": ""8/23/1967""
      },
      {
        ""ID"": ""3"",
        ""FName"": ""Joseph"",
        ""LName"": ""Doe"",
        ""DOB"": ""11/6/1994""
      }
    ]
  }
}",
             //settings:
             "CollectionNode|Members,RecordNode|Member,IndentChars|  "  //IndentChars relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_1
         #endregion Test_1


#region Test_2:
         //simple observe clusters
         _testDataRepo.Add("Test_2", Tuple.Create(
//inputJSON:
@"{
  ""Members"": {
    ""Family"": [
      {
        ""Member"": [
          {
            ""ID"": ""1"",
            ""FName"": ""Paul"",
            ""LName"": ""Smith"",
            ""DOB"": ""1/12/1988""
          }
        ]
      },
      {
        ""Member"": [
          {
            ""ID"": ""2"",
            ""FName"": ""John"",
            ""LName"": ""Green"",
            ""DOB"": ""8/23/1967""
          },
          {
            ""ID"": ""4"",
            ""FName"": ""Johnny"",
            ""LName"": ""Green"",
            ""DOB"": ""5/3/1997""
          }
        ]
      },
      {
        ""Member"": [
          {
            ""ID"": ""3"",
            ""FName"": ""Joseph"",
            ""LName"": ""Doe"",
            ""DOB"": ""11/6/1994""
          }
        ]
      }
    ]
  }
}",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member,IndentChars|  "  //IndentChars relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_2
#endregion Test_2


#region Test_3:
         //multi-level
         _testDataRepo.Add("Test_3", Tuple.Create(
//inputJSON:
@"{
  ""Members"": {
    ""X"": {
      ""Family"": {
        ""A"": [
          {
            ""X"": {
              ""B"": {
                ""Member"": [
                  {
                    ""ID"": ""1"",
                    ""FName"": ""Paul"",
                    ""LName"": ""Smith"",
                    ""DOB"": ""1/12/1988""
                  }
                ]
              }
            }
          },
          {
            ""X"": {
              ""B"": {
                ""Member"": [
                  {
                    ""ID"": ""2"",
                    ""FName"": ""John"",
                    ""LName"": ""Green"",
                    ""DOB"": ""8/23/1967""
                  },
                  {
                    ""ID"": ""4"",
                    ""FName"": ""Johnny"",
                    ""LName"": ""Green"",
                    ""DOB"": ""5/3/1997""
                  }
                ]
              }
            }
          },
          {
            ""X"": {
              ""B"": {
                ""Member"": [
                  {
                    ""ID"": ""3"",
                    ""FName"": ""Joseph"",
                    ""LName"": ""Doe"",
                    ""DOB"": ""11/6/1994""
                  }
                ]
              }
            }
          }
        ]
      }
    }
  }
}",
             //settings:
             "CollectionNode|Members,ClusterNode|X/Family/A,RecordNode|X/B/Member,IndentChars|  "  //IndentChars relevant only on output
                                                    ) //Tuple.Create
                          );  //Add Test_3
         #endregion Test_3

      }


      [DataTestMethod]
      [DataRow("Test_1")]
      [DataRow("Test_2")]
      [DataRow("Test_3")]
      public void Json_RoundTrip_CorrectData(string testCase)
      {
         //arrange
         var testData = _testDataRepo[testCase];
         var jsonText = testData.Item1;
         var settings = testData.Item2;
         var intakeReader = new StringReader(jsonText);

         _config.IntakeReader = () => intakeReader;
         _config.InputDataKind = KindOfTextData.JSON;
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = settings;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.JSON;
         _config.XmlJsonOutputSettings = settings;  //same as for intake
         _config.OutputWriter = () => _outputWriter;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         _outputData.ToString().Should().Be(jsonText);
      }
   }
}

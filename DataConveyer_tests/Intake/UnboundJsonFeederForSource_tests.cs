//UnboundJsonFeederForSource_tests.cs
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
using Mavidian.DataConveyer.Intake;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class UnboundJsonFeederForSource_tests
   {
      // Test data is kept in this dictionary:
      private Dictionary<string,               // testCase, i.e. key (e.g. SingleRecord
                         Tuple<string,         // input JSON
                               List<Xrecord>   // expected
                              >
                        > _testDataRepo;


      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<string, List<Xrecord>>>();

#region SingleRecord:
         //single record
         _testDataRepo.Add("SingleRecord", Tuple.Create(
//input:
@"{
      name: ""Jimmy"",
      city: ""New York"",
      age: 17
}",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add SingleRecord
#endregion SingleRecord
      }


      [DataTestMethod]
      [DataRow("SingleRecord")]
      public void UnboundJsonParsing_EndToEnd_CorrectData(string testCase)
      {
         //This is a series of end-to-end integration tests of unbound JSON parsing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputJSON = testData.Item1;
         var dummySourceNo = 42;
         var xrecordSupplier = new UnboundJsonFeederForSource(new StringReader(inputJSON), dummySourceNo);
         var xrecordSupplierPO = new PrivateObject(xrecordSupplier);
         var actual = new List<Xrecord>();
         var expected = testData.Item2;

         //act
         Xrecord elem;
         while ((elem = (Xrecord)xrecordSupplierPO.Invoke("SupplyNextXrecord")) != null)
         {
            actual.Add(elem);
         }

         //assert
         actual.Count.Should().Be(expected.Count);
         for (var i = 0; i < actual.Count; i++)
         {
            actual[i].Should().BeEquivalentTo(expected[i]);
         }
      }

   }
}

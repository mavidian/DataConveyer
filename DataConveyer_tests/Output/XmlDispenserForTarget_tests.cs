//XmlDispenserForTarget_tests.cs
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
using Mavidian.DataConveyer.Output;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;

namespace DataConveyer_tests.Output
{
   [TestClass]
   public class XmlDispenserForTarget_tests
   {
      // Test data is kept in this dictionary:
      private Dictionary<string,               // testCase, i.e. key (e.g. Members_1_
                         Tuple<List<Xrecord>,  // records to output
                               string,         // settings
                               string          // expected output
                              >
                        > _testDataRepo;


      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<List<Xrecord>, string, string>>();

#region Test_1
         //"Happy path" test
         _testDataRepo.Add("Test_1", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           )
             },
             //settings:
             "CollectionNode|Members,RecordNode|Member,IndentChars|   ",
             //expected output
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
</Members>"
                                                 )  //Tuple.Create
                          );  //Add Test_1
#endregion Test_1


#region Test_2
         //No IndentChars setting, i.e. no "pretty print"
         _testDataRepo.Add("Test_2", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
                           )
             },
             //settings:
             "CollectionNode|Members,RecordNode|Member",
             //expected output
             "<?xml version=\"1.0\" encoding=\"utf-16\"?><Members><Member><ID>1</ID><FName>Paul</FName><LName>Smith</LName><DOB>1/12/1988</DOB></Member></Members>"
                                                 )  //Tuple.Create
                          );  //Add Test_2
#endregion Test_2


#region Test_3
         //Unix line endings
         _testDataRepo.Add("Test_3", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
                           )
             },
             //settings:
             "CollectionNode|Members,RecordNode|Member,IndentChars|,NewLineChars|\n",  //needed for "pretty-print" i.e. new lines to be produced 
             //expected output
             "<?xml version=\"1.0\" encoding=\"utf-16\"?>\n<Members>\n<Member>\n<ID>1</ID>\n<FName>Paul</FName>\n<LName>Smith</LName>\n<DOB>1/12/1988</DOB>\n</Member>\n</Members>"
                                                 )  //Tuple.Create
                          );  //Add Test_3
#endregion Test_3


#region Test_4
         // no collection, i.e. XML fragment
         _testDataRepo.Add("Test_4", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           )
             },
             //settings:
             "RecordNode|Member,IndentChars|\t",
             //expected output
             @"<Member>
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
</Member>"
                                                 )  //Tuple.Create
                          );  //Add Test_4
#endregion Test_4


#region Test_5
         // respect clusters
         _testDataRepo.Add("Test_5", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member,IndentChars|\t",
             //expected output
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
</Members>"
                                                 )  //Tuple.Create
                          );  //Add Test_5
#endregion Test_5


#region Test_6
         // XML fragment, respect clusters
         _testDataRepo.Add("Test_6", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
             //settings:
             "ClusterNode|Family,RecordNode|Member,IndentChars|\t",
             //expected output
             @"<Family>
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
</Family>"
                                                 )  //Tuple.Create
                          );  //Add Test_6
         #endregion Test_6


#region Test_7
         // multi-level nodes with attributes
         _testDataRepo.Add("Test_7", Tuple.Create(
             //multi-level nodes
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("Weight","180" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("Weight","195" as object),
                                                               Tuple.Create("EyeColor","Brown" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
            //settings:
            "CollectionNode|Root/Members[@region=North],ClusterNode|Group[@id=2][@zone=\"\"]/Subgroup/Family,RecordNode|Data/Member[@class=\"main\"],IndentChars|  ",
             //expected output
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
            <Member class=""main"">
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
            <Member class=""main"">
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
</Root>"
                                                 )  //Tuple.Create
                          );  //Add Test_7
#endregion Test_7

 
#region Test_7a
         // multi-level nodes, no attributes
         _testDataRepo.Add("Test_7a", Tuple.Create(
             //multi-level nodes
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
            //settings:
            "CollectionNode|Members,ClusterNode|X/Family/A,RecordNode|X/B/Member,IndentChars|  ",
             //expected output
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
</Members>"
                                                 )  //Tuple.Create
                          );  //Add Test_7a
#endregion Test_7a


#region Test_8
         // attribute fields
         _testDataRepo.Add("Test_8", Tuple.Create(
             //multi-level nodes
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                                  Tuple.Create("FName","Paul" as object),
                                                                  Tuple.Create("LName","Smith" as object),
                                                                  Tuple.Create("Weight","180" as object),
                                                                  Tuple.Create("DOB", "1/12/1988" as object)
                                                                }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                                  Tuple.Create("FName","John" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "8/23/1967" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                                  Tuple.Create("FName","Johnny" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "5/3/1997" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                                  Tuple.Create("FName","Joseph" as object),
                                                                  Tuple.Create("LName","Doe" as object),
                                                                  Tuple.Create("Weight","195" as object),
                                                                  Tuple.Create("EyeColor","Brown" as object),
                                                                  Tuple.Create("DOB", "11/6/1994" as object)
                                                                }
, 3                            )
             },
             //settings:
             "CollectionNode|Root/Members[@region=North],ClusterNode|Group[@id=2][@zone=\"\"]/Subgroup/Family,RecordNode|Data/Member[@class=\"main\"],AttributeFields|ID;Dummy;Weight,IndentChars|  ",
             //expected output
             @"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Members region=""North"">
    <Group id=""2"" zone="""">
      <Subgroup>
        <Family>
          <Data>
            <Member class=""main"" ID=""1"" Weight=""180"">
              <FName>Paul</FName>
              <LName>Smith</LName>
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
            <Member class=""main"" ID=""2"">
              <FName>John</FName>
              <LName>Green</LName>
              <DOB>8/23/1967</DOB>
            </Member>
          </Data>
          <Data>
            <Member class=""main"" ID=""4"">
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
            <Member class=""main"" ID=""3"" Weight=""195"">
              <FName>Joseph</FName>
              <LName>Doe</LName>
              <EyeColor>Brown</EyeColor>
              <DOB>11/6/1994</DOB>
            </Member>
          </Data>
        </Family>
      </Subgroup>
    </Group>
  </Members>
</Root>"
                                                 )  //Tuple.Create
                          );  //Add Test_8
#endregion Test_8

      }  //Initialize


      [DataTestMethod]
      [DataRow("Test_1")]
      [DataRow("Test_2")]
      [DataRow("Test_3")]
      [DataRow("Test_4")]
      [DataRow("Test_5")]
      [DataRow("Test_6")]
      [DataRow("Test_7")]
      [DataRow("Test_7a")]
      [DataRow("Test_8")]
      public void XmlWriting_EndToEnd_CorrectData(string testCase)
      {
         //This is an end-to-end integration test of the XML parsing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputRecs = testData.Item1;
         var settings = testData.Item2;
         var dummyTargetNo = 15;
         var output = new StringWriter();
         var xrecordConsumer = new XmlDispenserForTarget(output, dummyTargetNo, settings, false);
         var expected = testData.Item3;

         //act
         inputRecs.ForEach(r => xrecordConsumer.SendNextLine(Tuple.Create((ExternalLine)r, dummyTargetNo)));
         xrecordConsumer.ConcludeDispensing();  //note that normally EOD marks are handled by LineDispenser
         var actual = output.ToString();

         //assert
         actual.Should().Be(expected);
      }


      [DataTestMethod]
      [DataRow("Test_1")]
      [DataRow("Test_2")]
      [DataRow("Test_3")]
      [DataRow("Test_4")]
      [DataRow("Test_5")]
      [DataRow("Test_6")]
      [DataRow("Test_7")]
      [DataRow("Test_7a")]
      [DataRow("Test_8")]
      public void XmlWritingAsync_EndToEnd_CorrectData(string testCase)
      {
         //This is an end-to-end integration test of the XML parsing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputRecs = testData.Item1;
         var settings = testData.Item2;
         var dummyTargetNo = 15;
         var output = new StringWriter();
         var xrecordConsumer = new XmlDispenserForTarget(output, dummyTargetNo, settings, true);
         var expected = testData.Item3;

         //act
         inputRecs.ForEach(async r => await xrecordConsumer.SendNextLineAsync(Tuple.Create((ExternalLine)r, dummyTargetNo)));
         xrecordConsumer.ConcludeDispensingAsync().Wait();  //note that normally EOD marks are handled by LineDispenser
         var actual = output.ToString();

         //assert
         actual.Should().Be(expected);
      }

   }
}

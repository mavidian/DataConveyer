//XmlFeederForSource_tests.cs
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

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class XmlFeederForSource_tests
   {
      // Test data is kept in this dictionary:
      private Dictionary<string,               // testCase, i.e. key (e.g. Members_1_
                         Tuple<string,         // inputXML
                               string,         // settings
                               List<Xrecord>   // expected
                              >
                        > _testDataRepo;


      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<string, string, List<Xrecord>>>();

#region Members_1.xml:
         _testDataRepo.Add("Members_1", Tuple.Create(
             //inputXML:
             @"<?xml version=""1.0"" encoding=""UTF-8"" ?>
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
             "CollectionNode|Members,RecordNode|Member,IncludeExplicitText|true",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1
#endregion Members_1.xml


#region Members_1a.xml
         _testDataRepo.Add("Members_1a", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Root>
	<Members>
		<Member>This contents will be ignored.</Member>
	</Members>
	<Members q=""good"">
		<Member>
			<ID>1</ID>
			<FName>Paul</FName>
			<LName>Smith</LName>
			<DOB>1/12/1988</DOB>
			<Empty1></Empty1>
			<Empty2/>
		</Member>
		<Member no=""2"">
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
	</Members>
	<Members q=""good"">
		<Member>This contents will be ignored.</Member>
	</Members>
</Root>",
             //settings:
             "CollectionNode|Root/Members[@q=\"good\"],RecordNode|Member,IncludeAttributes|truePlain",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("Empty1", string.Empty as object),
                                                               Tuple.Create("Empty2", string.Empty as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("no","2" as object),
                                                               Tuple.Create("ID","2" as object),
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1a
#endregion Members_1a.xml


#region Members_1b.xml
         // no collection, i.e. XML fragment
         _testDataRepo.Add("Members_1b", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
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
	<Member>Explicit Text here.</Member>",
             //settings:
             "RecordNode|Member,IncludeExplicitText|true",
             //expected:
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
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("__explicitText__","Explicit Text here." as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1b
#endregion Members_1b.xml


#region Members_1bE.xml
         // No matching records
         _testDataRepo.Add("Members_1bE", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
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
	<Member>Explicit Text here.</Member>",
             //settings:
             "RecordNode|BAD,IncludeExplicitText|true",
             //expected (empty set):
             new List<Xrecord>()
                                                    ) //Tuple.Create
                          );  //Add Members_1bE
#endregion Members_1bE.xml


#region Members_1c.xml:
         _testDataRepo.Add("Members_1c", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Root>
	<Distractor0/>
	<Distractor1></Distractor1>
	<Distractor2 a=""B"" c=""D"">blah</Distractor2>
  <Distractor2a/>
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
		<Distractor3>blah3</Distractor3>
		<Member>
			<ID>3</ID>
			<FName>Joseph</FName>
			<LName>Doe</LName>
			<DOB>11/6/1994</DOB>
		</Member>
	</Members>
	<Distractor4>blah4</Distractor4>
	<Members>
  	<Distractor5>blah5</Distractor5>
		<Member>This contents will be ignored.</Member>
	</Members>
  	<Distractor6>blah6</Distractor6>
</Root>",
             //settings:
             "CollectionNode|Root/Members,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1c
         #endregion Members_1c.xml


#region Members_1g.xml:
         //multi-level RecordNode
         _testDataRepo.Add("Members_1g", Tuple.Create(
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Members>
   <Member><X>
      <ID>1</ID>
      <FName>Paul</FName>
      <LName>Smith</LName>
      <DOB>1/12/1988</DOB>
   </X></Member>
   <Member><X>
      <ID>2</ID>
      <FName>John</FName>
      <LName>Green</LName>
      <DOB>8/23/1967</DOB>
   </X></Member>
   <Member><X>
      <ID>3</ID>
      <FName>Joseph</FName>
      <LName>Doe</LName>
      <DOB>11/6/1994</DOB>
   </X></Member>
</Members>",
             //settings:
             "CollectionNode|Members,RecordNode|Member/X,IncludeExplicitText|true",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1g
#endregion Members_1g.xml


#region Members_2.xml:
         //This test respects clusters and assigns them to resulting records (ClusterNode setting is present)
         _testDataRepo.Add("Members_2", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
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
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2
#endregion Members_2.xml


#region Members_2a.xml:
         //This test reads XML fragment and assigns clusters to resulting records (ClusterNode setting is present, but CollectionNode is absent)
         _testDataRepo.Add("Members_2a", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Family>
</Family>
<Family>
	<Member>
		<ID>1</ID>
		<FName>Paul</FName>
		<LName>Smith</LName>
		<DOB>1/12/1988</DOB>
	</Member>
</Family>
<Family/>
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
<Family><Dummy/></Family>",
         //settings:
         "ClusterNode|Family,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2a
         #endregion Members_2a.xml


#region Members_2e.xml:
         //This test respects clusters; multi-level ClusterNode
         _testDataRepo.Add("Members_2e", Tuple.Create(
             //inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Members>
	<Family><X>
		<Member>
			<ID>1</ID>
			<FName>Paul</FName>
			<LName>Smith</LName>
			<DOB>1/12/1988</DOB>
		</Member>
	</X></Family>
	<Family><X>
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
	</X></Family>
	<Family><X>
		<Member>
			<ID>3</ID>
			<FName>Joseph</FName>
			<LName>Doe</LName>
			<DOB>11/6/1994</DOB>
		</Member>
	</X></Family>
</Members>",
             //settings:
             "CollectionNode|Members,ClusterNode|Family/X,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2e
         #endregion Members_2e.xml


#region Members_2f.xml:
         //This test respects clusters; multi-level RecordNode
         _testDataRepo.Add("Members_2f", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Members>
	<Family>
		<X><Member>
			<ID>1</ID>
			<FName>Paul</FName>
			<LName>Smith</LName>
			<DOB>1/12/1988</DOB>
		</Member></X>
	</Family>
	<Family>
		<X><Member>
			<ID>2</ID>
			<FName>John</FName>
			<LName>Green</LName>
			<DOB>8/23/1967</DOB>
		</Member></X>
		<X><Member>
			<ID>4</ID>
			<FName>Johnny</FName>
			<LName>Green</LName>
			<DOB>5/3/1997</DOB>
		</Member></X>
	</Family>
	<Family>
		<X><Member>
			<ID>3</ID>
			<FName>Joseph</FName>
			<LName>Doe</LName>
			<DOB>11/6/1994</DOB>
		</Member></X>
	</Family>
</Members>",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|X/Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2f
#endregion Members_2f.xml


#region Members_2g.xml:
         //This test respects clusters; multi-level ClusterNode and RecordNode
         _testDataRepo.Add("Members_2g", Tuple.Create(
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
             "CollectionNode|Members,ClusterNode|X/Family/A,RecordNode|X/B/Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2g
#endregion Members_2g.xml


#region Members_2h.xml:
         //This test respects clusters; multi-level with attributes
         _testDataRepo.Add("Members_2h", Tuple.Create(
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
             "CollectionNode|Root/Members[@region=\"North\"],ClusterNode|Group[@id=2][@zone=\"\"]/Subgroup/Family,RecordNode|Data/Member[@class],IncludeAttributes|true",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@class","main" as object),
                                                               Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("Weight","180" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@class","main" as object),
                                                               Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@class","aux" as object),
                                                               Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@class","other" as object),
                                                               Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("Weight","195" as object),
                                                               Tuple.Create("EyeColor","Brown" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2h
#endregion Members_2h.xml


#region Members_3.xml:
         _testDataRepo.Add("Members_3", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Members>
	<Member ID=""1"">
		<FName>Paul</FName>
		<LName>Smith</LName>
		<DOB>1/12/1988</DOB>
	</Member>
	<Member ID=""2"">
		<FName>John</FName>
		<LName>Green</LName>
		<DOB>8/23/1967</DOB>
	</Member>
	<Member ID=""3"">
		<FName>Joseph</FName>
		<LName>Doe</LName>
		<DOB>11/6/1994</DOB>
	</Member>
</Members>",
             //settings:
             "CollectionNode|Members,RecordNode|Member,IncludeExplicitText|true,IncludeAttributes|truePlain",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_3
#endregion Members_3.xml


#region Members_4.xml:
         //This test demonstrates how explicit text is "glued" together (also dot notation of naming nested items)
         _testDataRepo.Add("Members_4", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Members>
	<Member ID=""1"">
		<Name>Jones<First  category=""special"" special=""dualName"">Paul<Suffix>Jr</Suffix>Mike</First>-Junior<Last>Smith</Last></Name>
		<DOB>1/12/1988</DOB>
		<Empty></Empty>
	</Member>
	<Dummy>Non-record node to be ignored</Dummy>
	<Member ID=""2"">Explicit <Name>
			<First>John</First>
			<Last>Green</Last>
		</Name>record text<Empty/>
		<DOB>8/23/1967</DOB>
	</Member>
	<Member ID=""3"">
		<Name>Donald Duck</Name>
	</Member>
	<Member ID=""4"">
		<Name>
			<First>Joseph</First>
			<Last>Doe</Last>
		</Name>Another explicit text<DOB>11/6/1994</DOB>	
	</Member>
</Members>",
             //settings:
             "CollectionNode|Members,RecordNode|Member,IncludeExplicitText|true,IncludeAttributes|true",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@ID","1" as object),
                                                               Tuple.Create("Name.First.Suffix","Jr" as object),
                                                               Tuple.Create("Name.First.@category","special" as object),
                                                               Tuple.Create("Name.First.@special","dualName" as object),
                                                               Tuple.Create("Name.First","PaulMike" as object),
                                                               Tuple.Create("Name.Last","Smith" as object),
                                                               Tuple.Create("Name","Jones-Junior" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("Empty", string.Empty as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@ID","2" as object),
                                                               Tuple.Create("Name.First","John" as object),
                                                               Tuple.Create("Name.Last","Green" as object),
                                                               Tuple.Create("Name",string.Empty as object),
                                                               Tuple.Create("Empty",string.Empty as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object),
                                                               Tuple.Create("__explicitText__", "Explicit record text" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@ID","3" as object),
                                                               Tuple.Create("Name","Donald Duck" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("@ID","4" as object),
                                                               Tuple.Create("Name.First","Joseph" as object),
                                                               Tuple.Create("Name.Last","Doe" as object),
                                                               Tuple.Create("Name",string.Empty as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object),
                                                               Tuple.Create("__explicitText__", "Another explicit text" as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_4
#endregion Members_4.xml


#region Members_5.xml:
         _testDataRepo.Add("Members_5", Tuple.Create(
//inputXML:
@"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Members>
	<Member>
		<ID>1</ID>
		<FName>Paul</FName>
		<LName>Smith</LName>
		<DOB>1/12/1988</DOB>
		<EmptyElems></EmptyElems>
		<EmptyElem />
		<EmptyElems />
		<Mixed>normal<b>bold</b>normal again</Mixed>
	</Member>
	<Member>
		<ID>2</ID>
		<FName>John</FName>
		<LName>Green</LName>
		<DOB>8/23/1967</DOB>
	</Member>
</Members>>",
             //settings:
             "CollectionNode|Members,RecordNode|Member,IncludeExplicitText|true",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("EmptyElems", string.Empty as object),
                                                               Tuple.Create("EmptyElem", string.Empty as object),
                                                               Tuple.Create("EmptyElems", string.Empty as object),
                                                               Tuple.Create("Mixed.b", "bold" as object),
                                                               Tuple.Create("Mixed", "normalnormal again" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_5
#endregion Members_5.xml


      }  //Initialize


      [DataTestMethod]
      [DataRow("Members_1")]
      [DataRow("Members_1a")]
      [DataRow("Members_1b")]
      [DataRow("Members_1bE")]
      [DataRow("Members_1c")]
      [DataRow("Members_1g")]
      [DataRow("Members_2")]
      [DataRow("Members_2a")]
      [DataRow("Members_2e")]
      [DataRow("Members_2f")]
      [DataRow("Members_2g")]
      [DataRow("Members_2h")]
      [DataRow("Members_3")]
      [DataRow("Members_4")]
      [DataRow("Members_5")]
      public void XmlParsing_EndToEnd_CorrectData(string testCase)
      {
         //This is a series of end-to-end integration tests of XML parsing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputXML = testData.Item1;
         var settings = testData.Item2;
         var dummySourceNo = 6;
         var xrecordSupplier = new XmlFeederForSource(new StringReader(inputXML), dummySourceNo, settings, false);
         var xrecordSupplierPO = new PrivateObject(xrecordSupplier);
         var actual = new List<Xrecord>();
         var expected = testData.Item3;

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


      [TestMethod]
      public void ReadToEnd__ConsumesAllInput()
      {
         //This end-to-end integration test of XML parsing verifies that EOD mark is supplied upon exhausting data on input

         //arrange
         var testData = _testDataRepo["Members_1"];
         var inputXML = testData.Item1;
         var settings = testData.Item2;
         var dummySourceNo = 6;
         var xrecordSupplier = new XmlFeederForSource(new StringReader(inputXML), dummySourceNo, settings, false);
         var xrecordSupplierPO = new PrivateObject(xrecordSupplier);
         var actual = new List<Xrecord>();
         var expected = testData.Item3;

         //act
         xrecordSupplier.ReadToEnd();
         var elem = xrecordSupplierPO.Invoke("SupplyNextXrecord") as Xrecord;

         //assert
         elem.Should().BeNull();
      }

   }
}

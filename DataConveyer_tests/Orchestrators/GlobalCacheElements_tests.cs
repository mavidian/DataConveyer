//GlobalCacheElements_tests.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace DataConveyer_tests.Orchestrators
{
   [TestClass]
   public class GlobalCacheElements_tests
   {
      OrchestratorConfig _cfg;

      [TestInitialize]
      public void Initialize()
      {                                                                                                                                                 
         _cfg = new OrchestratorConfig();         
      }


      [TestMethod]
      public void GlobalCacheElements_2SimpleSettings_NoElements()
      {
         //arrange
         _cfg.GlobalCacheElements = new string[] { "Elem1|0", "Elem2|abc" };
         object valToGet;

         //act
         var orchtr = (EtlOrchestrator)OrchestratorCreator.GetEtlOrchestrator(_cfg);
         var orchtrPO = new PrivateObject(orchtr);
         var gc = (IGlobalCache)orchtrPO.GetField("_globalCache");

         //assert
         gc.Count.Should().Be(2);
         gc.TryGet("Elem1", out valToGet).Should().BeTrue();
         valToGet.Should().Be(0);
         gc.TryGet("Elem2", out valToGet).Should().BeTrue();
         valToGet.Should().Be("abc");
         gc.TryGet("Elem3", out valToGet).Should().BeFalse();
      }


      [TestMethod]
      public void GlobalCacheElements_NoSetting_NoElements()
      {
         //arrange
         object valToGet;

         //act
         var orchtr = (EtlOrchestrator)OrchestratorCreator.GetEtlOrchestrator(_cfg);
         var orchtrPO = new PrivateObject(orchtr);
         var gc = (IGlobalCache)orchtrPO.GetField("_globalCache");

         //assert
         gc.Count.Should().Be(0);
         gc.TryGet("Elem1", out valToGet).Should().BeFalse();
         gc.TryGet("Elem2", out valToGet).Should().BeFalse();
         gc.TryGet("Elem3", out valToGet).Should().BeFalse();
      }


      [TestMethod]
      public void GlobalCacheElements_SingleSetting_OneElement()
      {
         //arrange
         _cfg.GlobalCacheElements = new string[] { "Elem1" };
         object valToGet;

         //act
         var orchtr = (EtlOrchestrator)OrchestratorCreator.GetEtlOrchestrator(_cfg);
         var orchtrPO = new PrivateObject(orchtr);
         var gc = (IGlobalCache)orchtrPO.GetField("_globalCache");

         //assert
         gc.Count.Should().Be(1);
         gc.TryGet("Elem3", out valToGet).Should().BeFalse();
         gc.TryGet("Elem1", out valToGet).Should().BeTrue();
         valToGet.Should().BeNull();
      }


      [TestMethod]
      public void GlobalCacheElements_VariousDataTypes_CorrectData()
      {
         //arrange
         _cfg.GlobalCacheElements = new string[] { "IntElem|0", "DecElem|0.", "DateElem|1/1/2011", "StrElem|abc" };
         object valToGet;

         //act
         var orchtr = (EtlOrchestrator)OrchestratorCreator.GetEtlOrchestrator(_cfg);
         var orchtrPO = new PrivateObject(orchtr);
         var gc = (IGlobalCache)orchtrPO.GetField("_globalCache");

         //assert
         gc.Count.Should().Be(4);
         gc.TryGet("Elem1", out valToGet).Should().BeFalse();
         gc.TryGet("IntElem", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<int>();
         valToGet.Should().Be(0);
         gc.TryGet("DecElem", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<decimal>();
         valToGet.Should().Be(0m);
         gc.TryGet("DateElem", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<DateTime>();
         valToGet.Should().Be(new DateTime(2011, 1, 1));
         gc.TryGet("StrElem", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be("abc");
      }


      [TestMethod]
      public void GlobalCacheElements_VariousTrickyData_CorrectData()
      {
         //arrange
         _cfg.GlobalCacheElements = new string[] { "IntElem|-1",
                                                       "DecElem|-32.44",
                                                       "DateElem|1-JAN-11",
                                                       "StrElem1|\"0\"",
                                                       "StrElem2|2/30/2011",
                                                       "StrElem3|\"\"a\",\"b\" and c\"",
                                                       "StrElem4|\"\"a\",\"b\" and c",  //ending quote is optional
                                                       "StrElem5",
                                                       "StrElem6|\"\"",
                                                       "StrElem7|",
                                                       "StrElem8|\" \"",
                                                       "StrElem9| " };
         object valToGet;

         //act
         var orchtr = (EtlOrchestrator)OrchestratorCreator.GetEtlOrchestrator(_cfg);
         var orchtrPO = new PrivateObject(orchtr);
         var gc = (IGlobalCache)orchtrPO.GetField("_globalCache");

         //assert
         gc.Count.Should().Be(12);
         gc.TryGet("BadElem", out valToGet).Should().BeFalse();
         gc.TryGet("IntElem", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<int>();
         valToGet.Should().Be(-1);
         gc.TryGet("DecElem", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<decimal>();
         valToGet.Should().Be(-32.44m);
         gc.TryGet("DateElem", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<DateTime>();
         valToGet.Should().Be(new DateTime(2011, 1, 1));
         gc.TryGet("StrElem1", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be("0");
         gc.TryGet("StrElem2", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be("2/30/2011");
         gc.TryGet("StrElem3", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be("\"a\",\"b\" and c");
         gc.TryGet("StrElem4", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be("\"a\",\"b\" and c");
         gc.TryGet("StrElem5", out valToGet).Should().BeTrue();
         valToGet.Should().BeNull();
         gc.TryGet("StrElem6", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be(string.Empty);
         gc.TryGet("StrElem7", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be(string.Empty);
         gc.TryGet("StrElem8", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be(" ");
         gc.TryGet("StrElem9", out valToGet).Should().BeTrue();
         valToGet.Should().BeOfType<string>();
         valToGet.Should().Be(" ");
      }
   }
}

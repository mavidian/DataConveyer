//XmlNodePath_tests.cs
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
using Mavidian.DataConveyer.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace DataConveyer_tests.Common
{
   [TestClass]
   public class XmlNodePath_tests
   {
      [TestMethod]
      public void ctor_EmptySpecs_NoNodeDefs()
      {
         //arrange
         var actual = new XmlNodePath(string.Empty);

         //act

         //assert
         actual.NodeDefs.Count.Should().Be(0);
      }

      [TestMethod]
      public void ctor_NullSpecs_NullNodeDefs()
      {
         //arrange
         var actual = new XmlNodePath(null as string);

         //act

         //assert
         actual.NodeDefs.Should().BeNull();
      }

      [TestMethod]
      public void ctor_SimpleSpecs_CorrectData()
      {
         //arrange
         var actual = new XmlNodePath("TheOnlyNode");

         //act
         var nodeDefs = actual.NodeDefs;

         //assert
         nodeDefs.Count.Should().Be(1);
         var nodeDef = nodeDefs[0];
         nodeDef.Name.Should().Be("TheOnlyNode");
         nodeDef.GetAttributes().Count.Should().Be(0);
      }

      [TestMethod]
      public void ctor_ComplexSpecs_CorrectData()
      {
         //arrange
         var actual = new XmlNodePath("./FirstNode[@id=1]//SecondNode");

         //act
         var nodeDefs = actual.NodeDefs;

         //assert
         nodeDefs.Count.Should().Be(2);
         var nodeDef = nodeDefs[0];
         nodeDef.Name.Should().Be("FirstNode");
         var attrs = nodeDef.GetAttributes();
         attrs.Count.Should().Be(1);
         attrs[0].Item1.Should().Be("id");
         attrs[0].Item2.Should().Be("1");
         nodeDef = nodeDefs[1];
         nodeDef.Name.Should().Be("SecondNode");
         nodeDef.GetAttributes().Count.Should().Be(0);
      }

      [TestMethod]
      public void IsEmpty_EmptyNode_ReturnsTrue()
      {
         //arrange
         var actual = new XmlNodePath(null as string);
         var actual2 = new XmlNodePath(null as List<XmlNodeDef>);
         var actual3 = new XmlNodePath(string.Empty);

         //act

         //assert
         actual.NodeDefs.Should().BeNull();
         actual.IsEmpty.Should().BeTrue();
         actual2.NodeDefs.Should().BeNull();
         actual2.IsEmpty.Should().BeTrue();
         actual3.NodeDefs.Count.Should().Be(0);  //empty string will result in empty list
         actual3.IsEmpty.Should().BeTrue();
      }
   }
}

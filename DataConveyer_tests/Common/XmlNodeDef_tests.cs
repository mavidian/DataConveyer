//XmlNodeDef_tests.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System;
using System.Collections.Generic;

namespace DataConveyer_tests.Common
{
   [TestClass]
   public class XmlNodeDef_tests
   {
      [TestMethod]
      public void ctor_emptySpecs_undetermined()
      {
         //arrange
         var actual = new XmlNodeDef(string.Empty);

         //act

         //assert
         actual.Name.Should().Be("__undetermined__");
         actual.GetAttributes().Should().BeOfType<List<Tuple<string, object>>>();
         actual.GetAttributes().Count.Should().Be(0);
      }

      [TestMethod]
      public void ctor_OnlyNameInSpecs_CorrectData()
      {
         //arrange
         var name = "MySpecialName";

         //act
         var actual = new XmlNodeDef(name);

         //assert
         actual.Name.Should().Be(name);
         actual.GetAttributes().Should().BeOfType<List<Tuple<string, object>>>();
         actual.GetAttributes().Count.Should().Be(0);
      }

      [TestMethod]
      public void ctor_OnlyAttribInSpecs_CorrectData()
      {
         //arrange
         var specs = "[@id][@key=\"x\"]";

         //act
         var actual = new XmlNodeDef(specs);
         var attribs = actual.GetAttributes();

         //assert
         actual.Name.Should().Be("__undetermined__");
         attribs.Should().BeOfType<List<Tuple<string, object>>>();
         attribs.Count.Should().Be(2);
         attribs[0].Item1.Should().Be("id");
         attribs[0].Item2.Should().BeNull();
         attribs[1].Item1.Should().Be("key");
         attribs[1].Item2.Should().Be("x");
      }

      [TestMethod]
      public void ctor_ComplexSpecs_CorrectData()
      {
         //arrange
         var specs = "MyName[@id][@key=\"x\"][@key2=x][@key=\"y\"]";

         //act
         var actual = new XmlNodeDef(specs);
         var attribs = actual.GetAttributes();

         //assert
         actual.Name.Should().Be("MyName");
         attribs.Should().BeOfType<List<Tuple<string, object>>>();
         attribs.Count.Should().Be(3);
         attribs[0].Item1.Should().Be("id");
         attribs[0].Item2.Should().BeNull();
         attribs[1].Item1.Should().Be("key");
         attribs[1].Item2.Should().Be("x");
         attribs[2].Item1.Should().Be("key2");
         attribs[2].Item2.Should().Be("x");  //attrib value can be surrounded in quotes or not
         //note that the 2nd key attribute was a dup and as such was rejected
      }

      [TestMethod]
      public void AddAttribute_UniqueKey_AttributeAdded()
      {
         //arrange
         var specs = "[@id][@key=\"x\"]";
         var actual = new XmlNodeDef(specs);

         //act
         actual.AddAttribute("new1", null);
         actual.AddAttribute("new2", "val2");
         var attribs = actual.GetAttributes();

         //assert
         actual.Name.Should().Be("__undetermined__");
         attribs.Should().BeOfType<List<Tuple<string, object>>>();
         attribs.Count.Should().Be(4);
         attribs[0].Item1.Should().Be("id");
         attribs[0].Item2.Should().BeNull();
         attribs[1].Item1.Should().Be("key");
         attribs[1].Item2.Should().Be("x");
         attribs[2].Item1.Should().Be("new1");
         attribs[2].Item2.Should().BeNull();
         attribs[3].Item1.Should().Be("new2");
         attribs[3].Item2.Should().Be("val2");
      }

      [TestMethod]
      public void AddAttribute_DupKey_AttributeIgnored()
      {
         //arrange
         var specs = "[@id][@key=\"x\"]";
         var actual = new XmlNodeDef(specs);

         //act
         actual.AddAttribute("id", "dummy");
         var attribs = actual.GetAttributes();

         //assert
         actual.Name.Should().Be("__undetermined__");
         attribs.Should().BeOfType<List<Tuple<string, object>>>();
         attribs.Count.Should().Be(2);
         attribs[0].Item1.Should().Be("id");
         attribs[0].Item2.Should().BeNull();
         attribs[1].Item1.Should().Be("key");
         attribs[1].Item2.Should().Be("x");
      }

      [TestMethod]
      public void GetAttributeValue_NoVsEmptyValue_CorrectDataReturned()
      {
         //arrange
         var specs = "[@NoVal][@EmptyVal1=][@EmptyVal2=\"\"][@SomeVal=blah]";
         var actual = new XmlNodeDef(specs);

         //act
        var attribs = actual.GetAttributes();

         //assert
         actual.Name.Should().Be("__undetermined__");
         attribs.Should().BeOfType<List<Tuple<string, object>>>();
         attribs.Count.Should().Be(4);
         attribs[0].Item1.Should().Be("NoVal");
         attribs[0].Item2.Should().BeNull();
         attribs[1].Item1.Should().Be("EmptyVal1");
         attribs[1].Item2.Should().Be(string.Empty);
         attribs[2].Item1.Should().Be("EmptyVal2");
         attribs[2].Item2.Should().Be(string.Empty);
         attribs[3].Item1.Should().Be("SomeVal");
         attribs[3].Item2.Should().Be("blah");

         actual.GetAttributeValue("BadAttr").Should().BeNull();  //non-existing attribute
         actual.AttributeExists("BadAttr").Should().BeFalse();

         actual.GetAttributeValue("NoVal").Should().BeNull();  //attribute with no value
         actual.AttributeExists("NoVal").Should().BeTrue();    //GetAttributeValue does not distinguish between non=existing attribute and attribute with no value, but AttributeExists does

         actual.GetAttributeValue("EmptyVal1").Should().Be(string.Empty);  //attribute with empty value
         actual.AttributeExists("EmptyVal1").Should().BeTrue();

         actual.GetAttributeValue("EmptyVal2").Should().Be(string.Empty);  //attribute with empty value
         actual.AttributeExists("EmptyVal2").Should().BeTrue();

         actual.GetAttributeValue("SomeVal").Should().Be("blah");  //attribute with non-empty value
         actual.AttributeExists("SomeVal").Should().BeTrue();
      }


      [TestMethod]
      public void GetAttributeDict_ComplexSpecs_CorrectData()
      {
         //arrange
         var specs = "MyName[@id][@key1=x][@key2=\"\"]";

         //act
         var actual = new XmlNodeDef(specs);
         var attribDict = actual.GetAttributeDict();

         //assert
         actual.Name.Should().Be("MyName");
         attribDict.Should().BeOfType<Dictionary<string, object>>();
         attribDict.Count.Should().Be(3);
         attribDict["id"].Should().BeNull();
         attribDict["key1"].Should().Be("x");
         attribDict["key2"].Should().Be(string.Empty);
      }

   }
}

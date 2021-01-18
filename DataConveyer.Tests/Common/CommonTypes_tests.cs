//CommonTypes_tests.cs
//
// Copyright © 2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System;
using System.Collections.Generic;
using Xunit;

namespace DataConveyer.Tests.Common
{
   public class CommonTypes_tests
   {
      [Fact]
      public void KindOfTextDataHelpers_Raw_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.Raw;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(false);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(false);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(false);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xtext);
      }


      [Fact]
      public void KindOfTextDataHelpers_Keyword_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.Keyword;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(false);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(true);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(false);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xtext);
      }


      [Fact]
      public void KindOfTextDataHelpers_Delimited_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.Delimited;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(true);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(true);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(true);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xtext);
      }


      [Fact]
      public void KindOfTextDataHelpers_Flat_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.Flat;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(true);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(false);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(true);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xtext);
      }


      [Fact]
      public void KindOfTextDataHelpers_Arbitrary_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.Arbitrary;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(false);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(false);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(false);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xtext);
      }


      [Fact]
      public void KindOfTextDataHelpers_XML_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.XML;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(false);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(true);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(false);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xrecord);
      }


      [Fact]
      public void KindOfTextDataHelpers_JSON_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.JSON;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(false);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(true);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(false);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xrecord);
      }


      [Fact]
      public void KindOfTextDataHelpers_UnboundJSON_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.UnboundJSON;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(false);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(true);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         sut.OutputFieldsAreNeededUpFront().Should().Be(false);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xrecord);
      }


      [Fact]
      public void KindOfTextDataHelpers_X12_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.X12;

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         sut.CanHaveHeaderRow().Should().Be(false);
         sut.OnTheFlyInputFieldsCanBeAllowed().Should().Be(true);
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(true);
         sut.OutputFieldsAreNeededUpFront().Should().Be(false);
         sut.ExternalLineType().Should().Be(ExternalLineType.Xsegment);
      }


      [Fact]
      public void KindOfTextDataHelpers_HL7_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.HL7;
         Action a = () => { var x = sut.CanHaveHeaderRow(); };


         //assert
         sut.Should().BeOfType<KindOfTextData>();
         a.Should().Throw<NotSupportedException>();
         a = () => { var x = sut.OnTheFlyInputFieldsCanBeAllowed(); };
         a.Should().Throw<NotSupportedException>();
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         a = () => { var x = sut.OutputFieldsAreNeededUpFront(); };
         a.Should().Throw<NotSupportedException>();
         sut.ExternalLineType().Should().Be(ExternalLineType.Xsegment);
      }


      [Fact]
      public void KindOfTextDataHelpers_Ultimate_CorrectValues()
      {
         //arrange, act
         var sut = KindOfTextData.Ultimate;
         Action a = () => { var x = sut.CanHaveHeaderRow(); };

         //assert
         sut.Should().BeOfType<KindOfTextData>();
         a.Should().Throw<NotSupportedException>();
         a = () => { var x = sut.OnTheFlyInputFieldsCanBeAllowed(); };
         a.Should().Throw<NotSupportedException>();
         sut.OnTheFlyInputFieldsAreAlwasyAllowed().Should().Be(false);
         a = () => { var x = sut.OutputFieldsAreNeededUpFront(); };
         a.Should().Throw<NotSupportedException>();
         sut.ExternalLineType().Should().Be(ExternalLineType.Xtext);
      }

   }
}
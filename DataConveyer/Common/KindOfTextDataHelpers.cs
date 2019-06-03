//KindOfTextDataHelpers.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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


using System;

namespace Mavidian.DataConveyer.Common
{
   /// <summary>
   /// Extension methods that add "characteristics" to the KindOfTextData enum
   /// This "characteristics" allows grouping of data kinds and simplifies
   /// behavior in intake/output processing by referring to the group
   /// rather than individual data type.
   /// </summary>
   internal static class KindOfTextDataHelpers
   {
      //TODO: Replace these extension methods by properties of the respective classes (derived from Intake/OutputProvider - Strategy pattern)

      /// <summary>
      /// Determine if the type supports possibility for column headers to be in the 1st row
      /// </summary>
      /// <param name="dataKind">Kind of text data (either input or output)</param>
      /// <returns>True if it is possible to have header row; false if not, in which case the HeadersInFirstInputRow or HeadersInFirstOutputRow setting is ignored</returns>
      internal static bool CanHaveHeaderRow(this KindOfTextData dataKind)
      {
         switch (dataKind)
         {
            case KindOfTextData.Raw:
            case KindOfTextData.Keyword:
            case KindOfTextData.Arbitrary:
            case KindOfTextData.X12:
            case KindOfTextData.XML:
            case KindOfTextData.JSON:
               //"speedy" data kinds, they can start output without knowing the output fields
               return false;
            case KindOfTextData.Delimited:
            case KindOfTextData.Flat:
               //"needy" data kinds, they can't start output unless all fields are known
               return true;
            default:
               //IMPORTANT: Make sure each newly implemented data kind is assigned to either "speedy" or "needy" category
               throw new NotSupportedException($"Unrecognized {dataKind} kind encountered.");
         }
      }


      /// <summary>
      /// Determine if is possible for the fields to be added "on the fly" (i.e. after the first row) on intake
      /// </summary>
      /// <param name="inputDataKind">Kind of input data</param>
      /// <returns>True when on-the-fly fields are allowed (assuming AllowOnTheFlyInputFields=true); false when not (regardless of AllowOnTheFlyInputFields)</returns>
      internal static bool OnTheFlyInputFieldsCanBeAllowed(this KindOfTextData inputDataKind)
      {
         switch (inputDataKind)
         {
            case KindOfTextData.Keyword:
            case KindOfTextData.Delimited:
            case KindOfTextData.X12:
            case KindOfTextData.XML:
            case KindOfTextData.JSON:
               //records of these kinds "can grow", i.e. it possible for fields to be added on the fly
               return true;
            case KindOfTextData.Raw:
            case KindOfTextData.Flat:
            case KindOfTextData.Arbitrary:
               //these kinds must have all fields fixed after 1st row (even if AllowOnTheFlyInputFields is true)
               return false;
            default:
               //IMPORTANT: Make sure each newly implemented data kind is assigned to either "fixed" or "can grow" category
               throw new NotSupportedException($"Unrecognized {inputDataKind} kind encountered.");
         }
      }


      /// <summary>
      /// Determine if data kind on intake must allow fields to be added "on the fly" (i.e. after the first row).
      /// Note that if true, then OnTheFlyInputFieldsCanBeAllowed must also be true.
      /// </summary>
      /// <param name="dataKind"></param>
      /// <returns>True means that on-the-fly fields must be allowed irrespective of AllowOnTheFlyInputFields); false means that on-the-fly fields may be disallowed via AllowOnTheFlyInputFields).</returns>
      internal static bool OnTheFlyInputFieldsAreAlwasyAllowed(this KindOfTextData dataKind)
      {
         switch (dataKind)
         {
            case KindOfTextData.X12:
               return true;
            default:
               return false;
         }
      }


      /// <summary>
      /// Determine if output processing needs to know all output fields before starting output
      /// </summary>
      /// <param name="outputDataKind">Kind of output data</param>
      /// <returns>True for "needy" kinds (output fields needed up-front), false for "speedy" data kinds (output doesn't need to know all fields at start)</returns>
      internal static bool OutputFieldsAreNeededUpFront(this KindOfTextData outputDataKind)
      {
         return outputDataKind.CanHaveHeaderRow();
         //both settings CanHaveHeaderRow and OutputFieldsAreNeededUpFront are synonymous, but they are kept separate for clarity of intents
      }


      /// <summary>
      /// Define the type of feeder/dispenser to be used on intake/ouput.
      /// </summary>
      /// <param name="dataKind"></param>
      /// <returns></returns>
      internal static ExternalLineType ExternalLineType(this KindOfTextData dataKind)
      {
         switch (dataKind)
         {
            case KindOfTextData.X12:
            case KindOfTextData.HL7:
               return Common.ExternalLineType.Xsegment;
            case KindOfTextData.XML:
            case KindOfTextData.JSON:
               return Common.ExternalLineType.Xrecord;
            default:
               return Common.ExternalLineType.Xtext;
         }
      }

   }
}

//OutputHelpers.cs
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


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// Helper methods such as extension methods to assist in output processing.
   /// </summary>
   internal static class OutputHelpers
   {
      /// <summary>
      /// Add surrounding quotes and escape quotes in string (if applicable); suitable for Delimited and Keyword (where separator/quote have special meanings), but not for Flat.
      /// </summary>
      /// <param name="value">An input string value</param>
      /// <param name="surroundWithQuotes">If false, then only quote a string that contains commas and/or quotes.</param>
      /// <param name="separator">Field separator, such as comma.</param>
      /// <returns>Input value possibly revised to be surrounded by quotes</returns>
      internal static string QuoteIfNeeded(this string value, bool surroundWithQuotes, char separator)
      {
         // if (value == null) return null;  //do nothing in case of null values!
         if (surroundWithQuotes || value.IndexOf(separator) > -1 || value.IndexOf('"') > -1)
         {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
         }
         else
         {
            return value;
         }
      }

      /// <summary>
      /// Add surrounding quotes and escape quotes in string (if applicable); suitable only for flat output (no special considerations to commas/quotes).
      /// </summary>
      /// <param name="value">An input string value</param>
      /// <param name="surroundWithQuotes">If true surround it, if false do not.</param>
      /// <returns>Input value possibly revised to be surrounded by quotes.</returns>
      internal static string QuoteIfNeededSimple(this string value, bool surroundWithQuotes)
      {
         // if (value == null) return null;  //do nothing in case of null values!
         if (surroundWithQuotes)
         {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
         }
         else
         {
            return value;
         }
      }


      /// <summary>
      /// Determines if output values should be surrounded with quotes or not.
      /// </summary>
      /// <param name="mode">One of: OnlyIfNeeded, StringsAnddates or Always.</param>
      /// <param name="type">Data type of output value.</param>
      /// <returns></returns>
      internal static bool SurroundWithQuotes(this QuotationMode mode, ItemType type)
      {
         switch (mode)
         {
            case QuotationMode.OnlyIfNeeded:
               return false; //in this case values containing commas and/or quotes will still be quoted
            case QuotationMode.StringsAndDates:
               return type == ItemType.String || type == ItemType.DateTime;
            default:
               return true;
         }
      }

   }
}

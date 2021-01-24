//X12OutputProvider.cs
//
// Copyright © 2017-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Output
{
   internal class X12OutputProvider : OutputProvider
   {
      private readonly X12Delimiters _x12Delimiters;
      private readonly Lazy<string> _x12FieldDelimiter;

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="x12Delimiters"></param>
      internal X12OutputProvider(OrchestratorConfig config, IGlobalCache globalCache, X12Delimiters x12Delimiters) : base(config, globalCache)
      {
         _x12Delimiters = x12Delimiters;
         _x12FieldDelimiter = new Lazy<string>(() => _x12Delimiters.X12FieldDelimiter == default(char) ? "*" : _x12Delimiters.X12FieldDelimiter.ToString());
      }

      /// <summary>
      /// In case of X12, a token is simply a string representation of the item's Value
      /// </summary>
      internal override Func<IItem, int, string> TokenFromItem
      {
         get
         {
            return (itm, sNo) => itm.StringValue;
         }
      }

      /// <summary>
      /// A function to splice tokens with a field delimiter in between
      /// </summary>
      internal override Func<IEnumerable<string>, ExternalLine> TokenJoiner
      {
         get
         {
            return (tkns) =>
            {
               if (tkns.FirstOrDefault() == "ISA")  //ISA segment has fixed size elements, which requires special handling
               {
                  var isaTkns = tkns.ToList();
                  if (isaTkns.Count != 17)  //16 ISA elements + 1 for ISA token itself
                  {
                     throw new DataMisalignedException($"Incorrect element count in X12 interchange envelope (ISA segment) - {isaTkns.Count - 1} instead of 16.");
                  }
                  var elemSizes = new int[] { 3, 2, 10, 2, 10, 2, 15, 2, 15, 6, 4, 1, 5, 9, 1, 1, 1 };  //per X12 specs
                  tkns = tkns.Zip(elemSizes, (t, s) => t?.PadRight(s)?.Substring(0, s) ?? new string(' ', s));  //account for a possible null value, e.g. assigned manually (treated the same way as empty string)
               }
               return ExternalLine.CreateXtext(string.Join(_x12FieldDelimiter.Value, tkns));
            };
         }
      }

      /// <summary>
      /// In case of X12, all items (fields) in current segment are included in output
      /// </summary>
      /// <param name="outputRecord"></param>
      /// <returns></returns>
      internal override IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord)
      {
         return outputRecord.Items;
      }

      /// <summary>
      /// This function should never be called in case of X12 output
      /// </summary>
      internal override Func<string, int, string> TokenForHeaderRow
      {
         get
         {
            throw new InvalidOperationException("Header Row must not be applied to X12 output.");
         }
      }


      internal override Func<IItem, Tuple<string, object>> ExtItemFromItem => i => throw new InvalidOperationException("X12OutputProvider must not use ExtItemFromItem function.");


      /// <summary>
      /// Feeds delimiters that were likely set during intake
      /// </summary>
      internal override Func<X12Delimiters> X12Delimiters { get { return () => _x12Delimiters; } }
   }
}


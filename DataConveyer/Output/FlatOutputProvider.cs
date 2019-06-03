//FlatOutputProvider.cs
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
using Mavidian.DataConveyer.Intake;        //to "borrow" extension methods (TrimIfNeeded, SafeSubstring) that reside in IntakeHelpers
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Mavidian.DataConveyer.Output
{
   internal class FlatOutputProvider : OutputProvider
   {
      private  IReadOnlyList<int> _fieldWidths;  //fixed field widths

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      internal FlatOutputProvider(OrchestratorConfig config, IGlobalCache globalCache) : base(config, globalCache)
      {
         if (config.OutputFields == null)
         {
            _fieldWidths = null;  //this will be overwritten based on actual set of fields used (SetFieldsToUse method)
         }
         else
         {  //this list of widths will not be overwritten
            _fieldWidths = config.OutputFields.ListOfSingleElements(1)
                         .ToListOfInts((config.DefaultOutputFieldWidth == 0) ? 10 : config.DefaultOutputFieldWidth)  //in case of undefined default width, 10 will be used
                         .ToList();
         }
      } //ctor


      /// <summary>
      /// Override specific to flat data adds determination of field widths for all fields (in case they were not specified in config)
      /// </summary>
      /// <param name="fieldsToUse"></param>
      internal override void SetFieldsToUse(IReadOnlyList<string> fieldsToUse)
      {
         base.SetFieldsToUse(fieldsToUse);

         if (_fieldWidths == null)  //do not overwrite the original set of widths (determined in ctor based on config)
         {
            _fieldWidths = fieldsToUse.Select(k => _config.DefaultOutputFieldWidth == 0 ? 10 : _config.DefaultOutputFieldWidth).ToList();
        }
      }


      /// <summary>
      /// A function to return a fixed width representation of the item's value (possibly trimmed and surrounded by quotes) - padded with spaces at end
      /// </summary>
      internal override Func<IItem, int, string> TokenFromItem
      {
         get
         {
            return (itm, sNo) =>
            {
               //itm - item to output (IKeyValItem)
               //sNo - index in _fieldWidths (int) 0-based
               Debug.Assert(itm.Key == base.FieldsToUse[sNo]);  //order of items is the same as FieldsToUse (and also _fieldWidths)
               return CreateFlatToken(itm.StringValue, this._fieldWidths[sNo], itm.ItemDef.Type);
            };
         }
      }

      /// <summary>
      /// Flat data TokenJoiner concatenates fields together (nothing in between)
      /// </summary>
      internal override Func<IEnumerable<string>, ExternalLine> TokenJoiner
      {
         get
         {
            return (tkns) =>
            {
               var line = string.Concat(tkns);
               return ExternalLine.CreateXtext(_config.ExcludeExtraneousFields ? line.TrimEnd() : line);
            };
         }
      }

      /// <summary>
      /// A list of items to include in output based on FieldsToUse (correspond to those specified in OutputFields)
      /// </summary>
      /// <param name="outputRecord"></param>
      /// <returns></returns>
      internal override IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord)
      {
         return base.ItemsFromFieldsToUse(outputRecord);
      }

      /// <summary>
      /// Returns a token to become a part of the header row
      /// </summary>
      internal override Func<string,int,string> TokenForHeaderRow
      {
         get
         {
            return (fn, sNo) => CreateFlatToken(fn, this._fieldWidths[sNo], ItemType.String);
         }
      }


      internal override Func<IItem, Tuple<string, object>> ExtItemFromItem => i => throw new InvalidOperationException("FlatOutputProvider must not use ExtItemFromItem function.");


      /// <summary>
      /// Helper function to take a "raw" token value along with the field width and apply needed trimming, quoting and formatting
      /// </summary>
      /// <param name="value"></param>
      /// <param name="width">Width of the output field</param>
      /// <param name="type">Data type of the output field</param>
      /// <returns></returns>
      private string CreateFlatToken(string value, int width, ItemType type)
      {
         bool swq = _config.QuotationMode.SurroundWithQuotes(type);
         var tkn = value?.TrimIfNeeded(_config.TrimOutputValues);
         return tkn.SafeSubstring(0, swq ? width - 2 : width).QuoteIfNeededSimple(swq).PadRight(width);
         //TODO: Make sure that field widths are > 2 if SurroundWithQuotes=true (part of "config scrubber")
         //TODO: Consider some graceful reaction to numeric values being too large to fit in the width (removing trailing digits may be confusing/undesirable,e.g. if formatted with leading zeros)
      }

   }
}

//ExternalLine.cs
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


//Contents:
// enum ExternalLineType
// class ExternalLine
// class Xtext
// class Xsegment
// class Xrecord

using Mavidian.DataConveyer.Intake;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Common
{
   /// <summary>
   /// Defines nature of data that DataConveyer receives from Intake or sends to Output.
   /// </summary>
   public enum ExternalLineType
   {
      /// <summary>
      /// A single line of text, i.e. string.
      /// Applies to textual data kinds, such as <see cref="KindOfTextData.Delimited"/>, <see cref="KindOfTextData.Flat"/>, etc.
      /// </summary>
      Xtext,
      /// <summary>
      /// A segment, such as X12 segment (future use; currently Xtext is used instead).
      /// </summary>
      Xsegment,
      /// <summary>
      /// A record containing items (key-value pairs) and cluster number.
      /// Applies to record-centric data, such as <see cref="KindOfTextData.XML"/> and <see cref="KindOfTextData.JSON"/> data kinds.
      /// </summary>
      Xrecord
   }

   /// <summary>
   /// A "unit of data" that Data Conveyer receives from Intake and sends to Output.
   /// It is a "sum-type" (also referred to as a discriminated union), which means objects of this class can take different forms represented by one of the derived types.
   /// Data Conveyer provides a <see cref="ExternalLineHelpers"/> class that contains a set of utilities, such as extension methods
   /// to help manipulate <see cref="ExternalLine"/> objects, 
   /// </summary>
   public abstract class ExternalLine
   {
      internal ExternalLine() { }

      /// <summary>
      /// Nature of data that Data Conveyer receives from Intake and sends to Output.
      /// </summary>
      public ExternalLineType Type { get; set; }

      internal static ExternalLine CreateXtext(string text)
      {
         return text == null ? null : new Xtext(text);
      }

      /// <summary>
      /// Sample contents of the object to help identifying it, e.g. in error messages.
      /// </summary>
      public virtual string Excerpt
      {
         get { return string.Empty;  }
      }

      /// <summary>
      /// Cluster number. Only meaningful in case of <see cref="Xrecord"/> subclass; otherwise 0.
      /// Note that this number is not the same as the <see cref="Entities.KeyVal.IRecordBase.ClstrNo"/> of the actual record (<see cref="Entities.KeyVal.IRecord"/>).
      /// </summary>
      public virtual int ClstrNo
      {
         get { return 0; }  // in case of Xrecord subclass, it may be assigned based on structure of intake records
      }

      /// <summary>
      /// Initial trace bin contents that may be obtained from XML nodes that represent clusters. Only meaningful in case of <see cref="Xrecord"/> subclass; otherwise null.
      /// </summary>
      public virtual IReadOnlyDictionary<string, object> TraceBin
      {
         get { return null; }  // in case of Xrecord subclass, it may be assigned based on structure of intake records
      }

      /// <summary>
      /// A single line of text. Only meaningful in case of <see cref="Xtext"/> subclass; otherwise null (Nothing in Visual Basic).
      /// </summary>
      public virtual string Text
      {
         get { return null; }  // to be overridden in Xtext subclass
      }

      /// <summary>
      /// Sequence of key-value pairs representing a record. Only meaningful in case of <see cref="Xrecord"/> subclass; otherwise null (Nothing in Visual Basic).
      /// </summary>
      public virtual IReadOnlyList<Tuple<string, object>> Items
      {
         get { return null; }  // to be overridden in Xrecord subclass
      }

   }

   /// <summary>
   /// One of the forms (derived types) of the <see cref="ExternalLine"/> type.
   /// Represents a single line of text sent into or out of the DataConveyer.
   /// Applicable to textual data kinds, such as <see cref="KindOfTextData.Delimited"/>, <see cref="KindOfTextData.Flat"/>, etc.
   /// </summary>
   public class Xtext : ExternalLine
   {
      private readonly string _text;
      /// <summary>
      /// A single line of text.
      /// </summary>
      public override string Text { get { return _text; } }
      /// <summary>
      /// First 15 chars except for leading whitespace
      /// </summary>
      public override string Excerpt { get { return _text.TrimStart().SafeSubstring(0, 15); } }

      internal Xtext(string text)
      {
         _text = text;
         Type = ExternalLineType.Xtext;
      }
   }


   /// <summary>
   /// One of the forms (derived types) of the <see cref="ExternalLine"/> type.
   /// Intended to represent X12 segment (future use, currently <see cref="KindOfTextData.X12"/> data kind is represented by <see cref="Xtext"/> type).
   /// </summary>
   public class Xsegment : ExternalLine
   {
   }


   /// <summary>
   /// One of the forms (derived classes) of the <see cref="ExternalLine"/> type.
   /// Represents record-like data sent into or out of Data Conveyer.
   /// Applicable to <see cref="KindOfTextData.XML"/> and <see cref="KindOfTextData.JSON"/> data kinds.
   /// </summary>
   public class Xrecord : ExternalLine
   {
      private readonly int _clstrNo;
      private readonly IReadOnlyList<Tuple<string, object>> _items;
      private readonly IReadOnlyDictionary<string, object> _traceBin;
      /// <summary>
      /// Cluster number associated with the record; 0 means undetermined.
      /// Data Conveyer assigns this number based on the ClusterNode value of the <see cref="Orchestrators.OrchestratorConfig.XmlJsonIntakeSettings"/> setting.
      /// This number is assigned sequentially, separately for each source.
      /// The <see cref="ClstrNo"/> is intended for evaluation (together with <see cref="Entities.KeyVal.IRecordBase.SourceNo"/>) by the <see cref="Orchestrators.OrchestratorConfig.ClusterMarker"/> function,
      /// which controls determination of the <see cref="Entities.KeyVal.IRecordBase.ClstrNo"/> of the record (<see cref="Entities.KeyVal.IRecord"/>) on intake.
      /// </summary>
      public override int ClstrNo { get { return _clstrNo; } }  //cluster counter/number (0 - undetermined)
      /// <summary>
      /// Initial trace bin contents that may be obtained from XML nodes that represent clusters.
      /// </summary>
      public override IReadOnlyDictionary<string, object> TraceBin { get { return _traceBin; } }
      /// <summary>
      /// A sequence of key-value pairs, each pair defining a single element (field) of the record; both key and value are strings.
      /// </summary>
      public override IReadOnlyList<Tuple<string, object>> Items { get { return _items; } }  //key-value pairs; alternatively KeyValuePair<string,object> (struct and not class) could be used
      /// <summary>
      /// First item in a form key=value.
      /// </summary>
      public override string Excerpt { get { return _items.Any() ? $"{_items[0].Item1}={_items[0].Item2}" : "<<empty>>"; } }
      internal Xrecord(List<Tuple<string, object>> items, int clstrNo, IDictionary<string, object> traceBin)
      {
         _clstrNo = clstrNo;
         _items = items;
         _traceBin = (IReadOnlyDictionary<string, object>)traceBin;
         Type = ExternalLineType.Xrecord;
      }
      internal Xrecord(List<Tuple<string, object>> items, int clstrNo) : this(items, clstrNo, null) { }
      internal Xrecord(List<Tuple<string, object>> items) : this(items, 0) { }
   }
}

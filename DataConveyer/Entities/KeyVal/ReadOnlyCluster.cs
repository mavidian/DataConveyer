//ReadOnlyCluster.cs
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


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Immutable wrapper over KeyValCluster
   /// </summary>
   internal class ReadOnlyCluster : ICluster
   {
      private readonly KeyValCluster _realCluster;
      private readonly bool _throwOnMisuse;

      internal ReadOnlyCluster(KeyValCluster realCluster, bool throwOnMisuse)
      {
         _realCluster = new KeyValCluster(realCluster.Records.Select(r => new ReadOnlyRecord((KeyValRecord)r,throwOnMisuse)), realCluster.ClstrNo, realCluster.StartRecNo, realCluster.StartSourceNo, realCluster.GlobalCache, realCluster.PropertyBin, realCluster._typeDefinitions, realCluster._config, realCluster._processingStatusSupplier);
         _throwOnMisuse = throwOnMisuse;
      }  //ctor

      internal ReadOnlyCluster(KeyValCluster realCluster) : this(realCluster, false) { }  //creates instance that ignores errors


#region ICluster implementation

      public int ClstrNo { get { return _realCluster.ClstrNo; } }

      public int StartRecNo { get { return _realCluster.StartRecNo; } }

      public int StartSourceNo { get { return _realCluster.StartSourceNo; } }

      public int Count { get { return _realCluster.Count; } }

      public IGlobalCache GlobalCache { get { return _realCluster.GlobalCache; } }  //note that elements of ReadOnlyCluster.GlobalCache are writable

      public IDictionary<string, object> PropertyBin { get { return _realCluster.PropertyBin; } }  //note that elements of ReadOnlyCluster.PropertyBin are writable

      public IReadOnlyList<IRecord> Records { get { return _realCluster.Records; } }

      public IRecord this[int index]
      {
         get { return _realCluster[index]; }
         set { ThrowOrIgnore(); }
      }

      public bool RemoveRecord(int index) { return ThrowOrIgnore(false); }

      public bool AddRecord(IRecord record) { return ThrowOrIgnore(false); }

      public IRecord GetRecord(int index) { return _realCluster.GetRecord(index); }

      public ICluster GetClone() { return ThrowOrIgnore<ICluster>(null); }

      public ICluster GetEmptyClone() { return ThrowOrIgnore<ICluster>(null); }

      public IRecord ObtainEmptyRecord() { return ThrowOrIgnore<IRecord>(null); }

      public PhaseStatus GetProcessingStatus(Phase phase, int transformerNo = 0) { return _realCluster.GetProcessingStatus(phase, transformerNo); }

#endregion ICluster implementation


      private T ThrowOrIgnore<T>(T value)
      {
         ThrowOrIgnore();
         return value;
      }

      private void ThrowOrIgnore()
      {
         if (_throwOnMisuse) throw new NotSupportedException("Unsupported operation invoked on ReadOnlyCluster object.");
      }

   }
}

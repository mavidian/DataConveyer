//RecordFilterTransformProvider.cs
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


using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Transform
{
   internal class RecordFilterTransformProvider : TransformProvider
   {

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="typeDefinitions"></param>
      /// <param name="config"></param>
      internal RecordFilterTransformProvider(TypeDefinitions typeDefinitions, OrchestratorConfig config) : base(typeDefinitions, config) { }

      
      internal override Func<KeyValCluster, IEnumerable<KeyValCluster>> Transform
      {
         get
         {
            //Every record in the cluster is evaluated using the supplied predicate (which can be a default "pass-through" predicate if not supplied in config).
            //If the predicate returns true, the record is added to the resulting cluster; otherwise, it is excluded.
            //As long as the resulting cluster (after applying the predicate to all records) contains at least one record, it (the resulting cluster) is returned in a sequence consisting of a single element;
            //However, if the resulting cluster contains no records, then an empty sequence is returned.
            return clstr =>
            {
               var recList = clstr.Records.Where(rec => _config.RecordFilterPredicate(rec)).Select(rec => (KeyValRecord)rec);
               return recList.Any() ? Enumerable.Repeat(new KeyValCluster(recList, clstr.ClstrNo, clstr.StartRecNo, clstr.StartSourceNo, clstr.GlobalCache, clstr.PropertyBin, _typeDefinitions, _config, clstr._processingStatusSupplier), 1) : Enumerable.Empty<KeyValCluster>();
               //TODO: Consider (??) AllowEmptyClusters config parameter, in which case (when true) empty cluster would be returned in case recList is empty, i.e. instead of return above simply do this:
               //      return Enumerable.Repeat(new KeyValCluster(recList, clstr.ClstrNo, clstr.StartRecNo, clstr.SourceNo, _config, clstr._processingStatusSupplier), 1);
            };
         }
      }
   }
}


//TransformProvider.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;

namespace Mavidian.DataConveyer.Transform
{
   /// <summary>
   /// Defines a set of functions that need to be supplied to the Transform part of the EtlOrchestrator
   /// </summary>
   internal abstract class TransformProvider
   {
      protected readonly TypeDefinitions _typeDefinitions;
      protected readonly OrchestratorConfig _config;

      private List<string> _fieldsInUse;
      private readonly HashSet<string> _fieldsInUseHashSet;  //efficiency feature to quickly recognize fields that are already in the list
      /// <summary>
      /// A sequence of unique field names processed during transformation (the order is not guaranteed due to multi-threading)
      /// </summary>
      internal IReadOnlyList<string> FieldsInUse { get { return _fieldsInUse.AsReadOnly(); } }

      private readonly object _locker = new object();

      protected TransformProvider(TypeDefinitions typeDefinitions, OrchestratorConfig config)
      {
         _typeDefinitions = typeDefinitions;
         _config = config;
         if (config.AllowTransformToAlterFields)
         {
            _fieldsInUse = new List<string>();
            _fieldsInUseHashSet = new HashSet<string>();
         }
         else  //transform will not calculate FieldsInUse
         {
            _fieldsInUse = null;  //null means "list will be copied from intake"
            //don't bother initializing _fieldsInUseHashSet, it won't be used 
         }

      } //ctor


      /// <summary>
      /// Factory method that returns a concrete instance of the derived class
      /// </summary>
      /// <param name="typeDefinitions"></param>
      /// <param name="config"></param>
      /// <returns></returns>
      internal static TransformProvider CreateProvider(TypeDefinitions typeDefinitions, OrchestratorConfig config)
      {
         switch(config.TransformerType)
         {
            case TransformerType.Clusterbound:
               return new ClusterboundTransformProvider(typeDefinitions, config);
            case TransformerType.Recordbound:
               return new RecordboundTransformProvider(typeDefinitions, config);
            case TransformerType.ClusterFilter:
               return new ClusterFilterTransformProvider(typeDefinitions, config);
            case TransformerType.RecordFilter:
               return new RecordFilterTransformProvider(typeDefinitions, config);
            case TransformerType.Aggregator:
               throw new NotImplementedException("Aggregator transformer has not yet been implemented.");
            default:  //TransformerType.Universal:
               return new UniversalTransformProvider(typeDefinitions, config);
         }
      }


      //Note on AllowTransformToAlterFields setting (default value is false):
      //There are two distinct outcomes from setting AllowTransformToAlterFields to true:
      //  (1) FieldsInUse list is dynamically recreated during the Transform phase (as opposed to be a simple copy of FieldsInUse from intake);
      //      this feature is implemented in TransformProvider (this) class (TransformAndSetFields).
      //  (2) Records processed during Transform phase can have fields added/removed (in case of default setting, no field additions/deletions are allowed);
      //      this feature is implemented in KeyValRecord class (FieldsCanBeAltered).
      //Setting the AllowTransformToAlterFields to true will impact performance and should only be set in case records need to be transformed to contain a different set
      // of fields on output (when compared with input records). In case transformation keeps the same set of fields, the setting should be left at the default value of false.


#region Functions common to all providers

      /// <summary>
      /// Assign a list of FieldsInUse (can only be assigned once, and only if AllowTransformToAlterFields is false)
      /// </summary>
      /// <param name="fieldsInUse"></param>
      internal void SetFieldsInUse(List<string> fieldsInUse)
      {
         //TODO: Not thread-safe, verify and fix if needed
         if (_fieldsInUse == null) _fieldsInUse = fieldsInUse;  //no action in case of subsequent attempt to set, original value retained
      }


      /// <summary>
      /// Execute transformer function and if needed update fields in use
      /// </summary>
      /// <param name="cluster">Cluster to transform</param>
      /// <returns></returns>
      internal IEnumerable<KeyValCluster> TransformAndSetFields(KeyValCluster cluster)
      {
         var clustersToReturn = Transform(cluster);

         if (!_config.AllowTransformToAlterFields) return clustersToReturn;

         //Here, we need to merge fields into FieldsInUse
         //clustersToReturn.ForEach.. would come in handy if it existed on IEnumerable :-(
         foreach (var clstr in clustersToReturn)
         {
            foreach(var rec in clstr.Records)
            {
               //TODO: Verify performance of this locking mechanism (consider changing its granularity)
               //      also, consider other thread synchronization methods (ConcurrentBag? instead of List)
               lock(_locker)
               {
                  MergeFields(rec.Keys);
               }
            }
         }

         return clustersToReturn;
      }

#endregion Functions common to all providers


#region Function(s) to be provided (overridden) by specific provider

      /// <summary>
      /// A function that takes a Cluster In and returns a sequence (0,1 or many) of Cluster Out
      /// </summary>
      internal abstract Func<KeyValCluster, IEnumerable<KeyValCluster>> Transform { get; }

#endregion Function(s) to be provided (overridden) by specific provider


#region Private methods

      /// <summary>
      /// Append given list of keys (field names) to _fieldsInUse
      /// </summary>
      /// <param name="fieldsToMerge">List of fields to join</param>
      private void MergeFields(IEnumerable<string> fieldsToMerge)
      {
         //not thread safe, but concurrency control is applied in calling code
         foreach (var fldName in fieldsToMerge)
         {
            if (_fieldsInUseHashSet.Add(fldName)) _fieldsInUse.Add(fldName);
         }
      }

#endregion Private methods

   }
}
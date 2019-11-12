// Copyright 2018 Owkin, inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"chaincode/errors"
	"fmt"
	"strconv"
)

// -------------------------------------------------------------------------------------------
// Methods on receivers traintuple
// -------------------------------------------------------------------------------------------

// SetFromInput is a method of the receiver AggregateTraintuple.
// It uses the inputAggregateTraintuple to check and set the aggregate traintuple's parameters
// which don't depend on previous traintuples values :
//  - AssetType
//  - Creator & permissions
//  - Tag
//  - AlgoKey & ObjectiveKey
func (traintuple *AggregateTraintuple) SetFromInput(db LedgerDB, inp inputAggregateTraintuple) error {
	creator, err := GetTxCreator(db.cc)
	if err != nil {
		return err
	}
	traintuple.AssetType = AggregateTraintupleType
	traintuple.Creator = creator
	traintuple.Tag = inp.Tag
	algo, err := db.GetAggregateAlgo(inp.AlgoKey)
	if err != nil {
		return errors.BadRequest(err, "could not retrieve algo with key %s", inp.AlgoKey)
	}
	if !algo.Permissions.CanProcess(algo.Owner, creator) {
		return errors.Forbidden("not authorized to process algo %s", inp.AlgoKey)
	}
	traintuple.AlgoKey = inp.AlgoKey

	// check objective exists
	objective, err := db.GetObjective(inp.ObjectiveKey)
	if err != nil {
		return errors.BadRequest(err, "could not retrieve objective with key %s", inp.ObjectiveKey)
	}
	if !objective.Permissions.CanProcess(objective.Owner, creator) {
		return errors.Forbidden("not authorized to process objective %s", inp.ObjectiveKey)
	}
	traintuple.ObjectiveKey = inp.ObjectiveKey

	// TODO (aggregate): uncomment + add test
	// traintuple.Permissions = MergePermissions(dataManager.Permissions, algo.Permissions)

	traintuple.Worker = inp.Worker
	return nil
}

// SetFromParents set the status of the aggregate traintuple depending on its "parents",
// i.e. the traintuples from which it received the outModels as inModels.
// Also it's InModelKeys are set.
func (traintuple *AggregateTraintuple) SetFromParents(db LedgerDB, inModels []string) error {
	status := StatusTodo
	for _, parentTraintupleKey := range inModels {
		// TODO (aggregate): support other types of parents
		parentTraintuple, err := db.GetAggregateTraintuple(parentTraintupleKey)
		if err != nil {
			err = errors.BadRequest(err, "could not retrieve parent traintuple with key %s %d", inModels, len(inModels))
			return err
		}
		// set traintuple to waiting if one of the parent traintuples is not done
		if parentTraintuple.OutModel == nil {
			status = StatusWaiting
		}
		traintuple.InModelKeys = append(traintuple.InModelKeys, parentTraintupleKey)
	}
	traintuple.Status = status
	return nil
}

// GetKey return the key of the aggregate traintuple depending on its key parameters.
func (traintuple *AggregateTraintuple) GetKey() string {
	hashKeys := []string{traintuple.Creator, traintuple.AlgoKey}
	hashKeys = append(hashKeys, traintuple.InModelKeys...)
	return HashForKey("aggregate-traintuple", hashKeys...)
}

// AddToComputePlan set the aggregate traintuple's parameters that determines if it's part of on ComputePlan and how.
// It uses the inputAggregateTraintuple values as follow:
//  - If neither ComputePlanID nor rank is set it returns immediately
//  - If rank is 0 and ComputePlanID empty, it's start a new one using this traintuple key
//  - If rank and ComputePlanID are set, it checks if there are coherent with previous ones and set it.
func (traintuple *AggregateTraintuple) AddToComputePlan(db LedgerDB, inp inputAggregateTraintuple, traintupleKey string) error {
	// check ComputePlanID and Rank and set it when required
	var err error
	if inp.Rank == "" {
		if inp.ComputePlanID != "" {
			return errors.BadRequest("invalid inputs, a ComputePlan should have a rank")
		}
		return nil
	}
	traintuple.Rank, err = strconv.Atoi(inp.Rank)
	if err != nil {
		return err
	}
	if inp.ComputePlanID == "" {
		if traintuple.Rank != 0 {
			err = errors.BadRequest("invalid inputs, a new ComputePlan should have a rank 0")
			return err
		}
		traintuple.ComputePlanID = traintupleKey
		return nil
	}
	var ttKeys []string
	// TODO (aggregate): does any of this make sense for aggregate ?
	ttKeys, err = db.GetIndexKeys("aggregateTraintuple~computeplanid~worker~rank~key", []string{"aggregateTraintuple", inp.ComputePlanID})
	if err != nil {
		return err
	}
	if len(ttKeys) == 0 {
		return errors.BadRequest("cannot find the ComputePlanID %s", inp.ComputePlanID)
	}
	for _, ttKey := range ttKeys {
		FLTraintuple, err := db.GetAggregateTraintuple(ttKey)
		if err != nil {
			return err
		}
		if FLTraintuple.AlgoKey != inp.AlgoKey {
			return errors.BadRequest("previous traintuple for ComputePlanID %s does not have the same algo key %s", inp.ComputePlanID, inp.AlgoKey)
		}
	}

	ttKeys, err = db.GetIndexKeys("aggregateTraintuple~computeplanid~worker~rank~key", []string{"aggregateTraintuple", inp.ComputePlanID, traintuple.Worker, inp.Rank})
	if err != nil {
		return err
	} else if len(ttKeys) > 0 {
		err = errors.BadRequest("ComputePlanID %s with worker %s rank %d already exists", inp.ComputePlanID, traintuple.Worker, traintuple.Rank)
		return err
	}

	traintuple.ComputePlanID = inp.ComputePlanID

	return nil
}

// Save will put in the legder interface both the aggregate traintuple with its key
// and all the associated composite keys
func (traintuple *AggregateTraintuple) Save(db LedgerDB, traintupleKey string) error {

	// store in ledger
	if err := db.Add(traintupleKey, traintuple); err != nil {
		return err
	}

	// create composite keys
	if err := db.CreateIndex("aggregateTraintuple~algo~key", []string{"aggregateTraintuple", traintuple.AlgoKey, traintupleKey}); err != nil {
		return err
	}
	if err := db.CreateIndex("aggregateTraintuple~worker~status~key", []string{"aggregateTraintuple", traintuple.Worker, traintuple.Status, traintupleKey}); err != nil {
		return err
	}
	for _, inModelKey := range traintuple.InModelKeys {
		if err := db.CreateIndex("aggregateTraintuple~inModel~key", []string{"aggregateTraintuple", inModelKey, traintupleKey}); err != nil {
			return err
		}
	}
	if traintuple.ComputePlanID != "" {
		if err := db.CreateIndex("aggregateTraintuple~computeplanid~worker~rank~key", []string{"aggregateTraintuple", traintuple.ComputePlanID, traintuple.Worker, strconv.Itoa(traintuple.Rank), traintupleKey}); err != nil {
			return err
		}
	}
	if traintuple.Tag != "" {
		err := db.CreateIndex("aggregateTraintuple~tag~key", []string{"aggregateTraintuple", traintuple.Tag, traintupleKey})
		if err != nil {
			return err
		}
	}
	return nil
}

// -------------------------------------------------------------------------------------------
// Smart contracts related to aggregate traintuples
// -------------------------------------------------------------------------------------------

// createAggregateTraintuple adds a AggregateTraintuple in the ledger
func createAggregateTraintuple(db LedgerDB, args []string) (map[string]string, error) {
	inp := inputAggregateTraintuple{}
	err := AssetFromJSON(args, &inp)
	if err != nil {
		return nil, err
	}

	traintuple := AggregateTraintuple{}
	err = traintuple.SetFromInput(db, inp)
	if err != nil {
		return nil, err
	}
	err = traintuple.SetFromParents(db, inp.InModels)
	if err != nil {
		return nil, err
	}

	traintupleKey := traintuple.GetKey()
	// Test if the key (ergo the traintuple) already exists
	tupleExists, err := db.KeyExists(traintupleKey)
	if err != nil {
		return nil, err
	}
	if tupleExists {
		return nil, errors.Conflict("traintuple already exists").WithKey(traintupleKey)
	}
	err = traintuple.AddToComputePlan(db, inp, traintupleKey)
	if err != nil {
		return nil, err
	}
	err = traintuple.Save(db, traintupleKey)
	if err != nil {
		return nil, err
	}
	out := outputAggregateTraintuple{}
	err = out.Fill(db, traintuple, traintupleKey)
	if err != nil {
		return nil, err
	}

	// TODO (aggregate): uncomment and add test
	// event := TuplesEvent{}
	// event.SetCompositeTraintuples(out)
	// err = SendTuplesEvent(db.cc, event)
	// if err != nil {
	// 	return nil, err
	// }

	return map[string]string{"key": traintupleKey}, nil
}

// logStartAggregateTrain modifies a traintuple by changing its status from todo to doing
func logStartAggregateTrain(db LedgerDB, args []string) (outputTraintuple outputAggregateTraintuple, err error) {
	inp := inputHash{}
	err = AssetFromJSON(args, &inp)
	if err != nil {
		return
	}

	// get traintuple, check validity of the update
	traintuple, err := db.GetAggregateTraintuple(inp.Key)
	if err != nil {
		return
	}
	if err = validateTupleOwner(db, traintuple.Worker); err != nil {
		return
	}
	if err = traintuple.commitStatusUpdate(db, inp.Key, StatusDoing); err != nil {
		return
	}
	outputTraintuple.Fill(db, traintuple, inp.Key)
	return
}

// logFailAggregateTrain modifies a traintuple by changing its status to fail and reports associated logs
func logFailAggregateTrain(db LedgerDB, args []string) (outputTraintuple outputAggregateTraintuple, err error) {
	inp := inputLogFailTrain{}
	err = AssetFromJSON(args, &inp)
	if err != nil {
		return
	}

	// get, update and commit traintuple
	traintuple, err := db.GetAggregateTraintuple(inp.Key)
	if err != nil {
		return
	}
	traintuple.Log += inp.Log

	if err = validateTupleOwner(db, traintuple.Worker); err != nil {
		return
	}
	if err = traintuple.commitStatusUpdate(db, inp.Key, StatusFailed); err != nil {
		return
	}

	outputTraintuple.Fill(db, traintuple, inp.Key)

	// update depending tuples
	event := TuplesEvent{}
	// TODO (ask Camille?): What type of children can aggregate traintuples have?
	// err = traintuple.updateTesttupleChildren(db, inp.Key, &event)
	// if err != nil {
	// 	return
	// }

	// err = traintuple.updateTraintupleChildren(db, inp.Key, &event)
	// if err != nil {
	// 	return
	// }

	err = SendTuplesEvent(db.cc, event)
	if err != nil {
		return
	}

	return
}

// logSuccessAggregateTrain modifies a traintuple by changing its status from doing to done
// reports logs and associated performances
func logSuccessAggregateTrain(db LedgerDB, args []string) (outputTraintuple outputAggregateTraintuple, err error) {
	inp := inputLogSuccessTrain{}
	err = AssetFromJSON(args, &inp)
	if err != nil {
		return
	}
	traintupleKey := inp.Key

	// get, update and commit traintuple
	traintuple, err := db.GetAggregateTraintuple(traintupleKey)
	if err != nil {
		return
	}
	traintuple.OutModel = &HashDress{
		Hash:           inp.OutModel.Hash,
		StorageAddress: inp.OutModel.StorageAddress}
	traintuple.Log += inp.Log

	if err = validateTupleOwner(db, traintuple.Worker); err != nil {
		return
	}
	if err = traintuple.commitStatusUpdate(db, traintupleKey, StatusDone); err != nil {
		return
	}

	// TODO (ask Camille?): What type of children can aggregate traintuples have?
	event := TuplesEvent{}
	// err = traintuple.updateTraintupleChildren(db, traintupleKey, &event)
	// if err != nil {
	// 	return
	// }

	// err = traintuple.updateTesttupleChildren(db, traintupleKey, &event)
	// if err != nil {
	// 	return
	// }

	outputTraintuple.Fill(db, traintuple, inp.Key)
	err = SendTuplesEvent(db.cc, event)
	if err != nil {
		return
	}

	return
}

// queryAggregateTraintuple returns info about an aggregate traintuple given its key
func queryAggregateTraintuple(db LedgerDB, args []string) (outputTraintuple outputAggregateTraintuple, err error) {
	inp := inputHash{}
	err = AssetFromJSON(args, &inp)
	if err != nil {
		return
	}
	traintuple, err := db.GetAggregateTraintuple(inp.Key)
	if err != nil {
		return
	}
	if traintuple.AssetType != AggregateTraintupleType {
		err = errors.NotFound("no element with key %s", inp.Key)
		return
	}
	outputTraintuple.Fill(db, traintuple, inp.Key)
	return
}

// queryAggregateTraintuples returns all aggregate traintuples
func queryAggregateTraintuples(db LedgerDB, args []string) ([]outputAggregateTraintuple, error) {
	outTraintuples := []outputAggregateTraintuple{}

	if len(args) != 0 {
		err := errors.BadRequest("incorrect number of arguments, expecting nothing")
		return outTraintuples, err
	}
	elementsKeys, err := db.GetIndexKeys("aggregateTraintuple~algo~key", []string{"aggregateTraintuple"})
	if err != nil {
		return outTraintuples, err
	}
	for _, key := range elementsKeys {
		outputTraintuple, err := getOutputAggregateTraintuple(db, key)
		if err != nil {
			return outTraintuples, err
		}
		outTraintuples = append(outTraintuples, outputTraintuple)
	}
	return outTraintuples, nil
}

// ----------------------------------------------------------
// Utils for smartcontracts related to aggregate traintuples
// ----------------------------------------------------------

// getOutputAggregateTraintuple takes as input a traintuple key and returns the outputAggregateTraintuple
func getOutputAggregateTraintuple(db LedgerDB, traintupleKey string) (outTraintuple outputAggregateTraintuple, err error) {
	traintuple, err := db.GetAggregateTraintuple(traintupleKey)
	if err != nil {
		return
	}
	outTraintuple.Fill(db, traintuple, traintupleKey)
	return
}

// getOutputAggregateTraintuples takes as input a list of keys and returns a paylaod containing a list of associated retrieved elements
func getOutputAggregateTraintuples(db LedgerDB, traintupleKeys []string) (outTraintuples []outputAggregateTraintuple, err error) {
	for _, key := range traintupleKeys {
		var outputTraintuple outputAggregateTraintuple
		outputTraintuple, err = getOutputAggregateTraintuple(db, key)
		if err != nil {
			return
		}
		outTraintuples = append(outTraintuples, outputTraintuple)
	}
	return
}

// commitStatusUpdate update the traintuple status in the ledger
func (traintuple *AggregateTraintuple) commitStatusUpdate(db LedgerDB, traintupleKey string, newStatus string) error {
	if traintuple.Status == newStatus {
		return fmt.Errorf("cannot update traintuple %s - status already %s", traintupleKey, newStatus)
	}

	if err := traintuple.validateNewStatus(db, newStatus); err != nil {
		return fmt.Errorf("update traintuple %s failed: %s", traintupleKey, err.Error())
	}

	oldStatus := traintuple.Status
	traintuple.Status = newStatus
	if err := db.Put(traintupleKey, traintuple); err != nil {
		return fmt.Errorf("failed to update traintuple %s - %s", traintupleKey, err.Error())
	}

	// update associated composite keys
	indexName := "aggregateTraintuple~worker~status~key"
	oldAttributes := []string{"aggregateTraintuple", traintuple.Worker, oldStatus, traintupleKey}
	newAttributes := []string{"aggregateTraintuple", traintuple.Worker, traintuple.Status, traintupleKey}
	if err := db.UpdateIndex(indexName, oldAttributes, newAttributes); err != nil {
		return err
	}
	logger.Infof("traintuple %s status updated: %s (from=%s)", traintupleKey, newStatus, oldStatus)
	return nil
}

// validateNewStatus verifies that the new status is consistent with the tuple current status
func (traintuple *AggregateTraintuple) validateNewStatus(db LedgerDB, status string) error {
	// check validity of worker and change of status
	if err := checkUpdateTuple(db, traintuple.Worker, traintuple.Status, status); err != nil {
		return err
	}
	return nil
}

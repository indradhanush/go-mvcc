package main

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/tidwall/btree"
)

// Scaffolding
func assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func assertEq[C comparable](a C, b C, prefix string) {
	if a != b {
		panic(fmt.Sprintf("%s '%v' != '%v'", prefix, a, b))
	}
}

var DEBUG = slices.Contains(os.Args, "--debug")

func debug(a ...any) {
	if !DEBUG {
		return
	}

	args := append([]any{"[DEBUG] "}, a...)
	fmt.Println(args...)
}

// A DB value will have a start and end TX ID (TODO: Needs clarification on start and end).
type Value struct {
	txStartID uint64
	txEndID   uint64
	value     string
}

type TransactionState uint8

// A transaction can be in one of the 3 following states.
const (
	InProgressTransaction TransactionState = iota
	AbortedTransaction
	CommittedTransaction
)

type IsolationLevel uint8

// We want to support the following major isolation levels, ordered in increasing strictness.
const (
	ReadUncommittedIsolation IsolationLevel = iota
	ReadCommittedIsolation
	RepeatableReadIsolation
	SnapshotIsolation
	SerializableIsolation
)

// A Transaction will have an ID (monoticially increasing), a known isolation level and a transaction state.
type Transaction struct {
	id        uint64
	isolation IsolationLevel
	state     TransactionState

	// A list of inProgress transactions.
	//
	// In my understanding so far, this will provide this transaction with a "view" into the
	// currently running transactions and possibly help decide if it wants to wait for another
	// transaction or not before reading or writing data.

	// Used only by Repeatable Read and stricter isolation levels.
	inProgress btree.Set[uint64]

	// Used only by Snapshot Isolation and stricter isolation levels.
	// TODO: Understand what this is used for.
	writeset btree.Set[string]
	readset  btree.Set[string]
}

type Database struct {
	// By default, this is the isolation level that will be applied on all transsactions.
	defaultIsolation IsolationLevel

	// The store is a key-value store and stores newer values in an append-only (TODO: verify this) slice.
	store map[string][]Value

	// An ordered list of transactions IDs
	transactions btree.Map[uint64, Transaction]

	// The next available transaction ID.
	nextTransactionID uint64
}

func newDatabase() Database {
	return Database{
		defaultIsolation: ReadCommittedIsolation,
		store:            map[string][]Value{},

		// Transaction ID '0' will imply that the ID was not set.
		// '1' is the first valid Tx ID.
		//
		// See: assertValidTransaction.
		nextTransactionID: 1,
	}
}

// We want to iterate through all transactions and return a list of transactions in progress.
func (d *Database) inProgress() btree.Set[uint64] {
	var ids btree.Set[uint64]
	iter := d.transactions.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if iter.Value().state == InProgressTransaction {
			ids.Insert(iter.Key())
		}
	}

	return ids
}

func (d *Database) newTransaction() *Transaction {
	t := Transaction{
		isolation: d.defaultIsolation,
		state:     InProgressTransaction,
		id:        d.nextTransactionID,
	}
	t.inProgress = d.inProgress()

	d.nextTransactionID += 1
	d.transactions.Set(t.id, t)

	debug("starting transaction", t.id)

	return &t
}

func (d *Database) completeTransaction(t *Transaction, state TransactionState) error {
	debug("completing transactoin", t.id)

	// Either the transaction is to be marked as aborted or completed at this stage.
	t.state = state
	d.transactions.Set(t.id, *t)

	return nil
}

func (d *Database) transactionState(txID uint64) Transaction {
	t, ok := d.transactions.Get(txID)
	assert(ok, "valid transacation")

	return t

}

func (d *Database) assertValidTransaction(t *Transaction) {
	assert(t.id > 0, "valid id")
	assert(d.transactionState(t.id).state == InProgressTransaction, "in progress transaction")
}

// A connection is a reference to a database and may or may not have an associated transaction with it.
type Connection struct {
	tx *Transaction
	db *Database
}

func (c *Connection) execCommand(command string, args []string) (string, error) {

	command = strings.ToLower(command)
	debug(command, args)

	// A user can start a transaction by using the command: BEGIN
	if command == "begin" {
		assertEq(c.tx, nil, "no running transactions")
		c.tx = c.db.newTransaction()
		c.db.assertValidTransaction(c.tx)
		return fmt.Sprintf("%d", c.tx.id), nil
	}

	if command == "abort" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, AbortedTransaction)
		c.tx = nil
		return "", err
	}

	if command == "commit" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, CommittedTransaction)
		c.tx = nil
		return "", err
	}

	if command == "get" {
		c.db.assertValidTransaction(c.tx)
		key := args[0]

		// TODO: What's going on here? Snapshot isolation?
		//
		// We track the key being read. TODO: Why?
		c.tx.readset.Insert(key)

		for i := len(c.db.store[key]) - 1; i > 0; i-- {
			value := c.db.store[key][i]
			// TODO: isvisibile needs to be implemented.
			debug(value, c.tx, c.db.isVisible(c.tx, value))
			if c.db.isVisible(c.tx, value) {
				return value.value, nil
			}
		}
	}

	if command == "set" || command == "delete" {
		c.db.assertValidTransaction(c.tx)
		key := args[0]

		found := false
		// Any existing versions should be marked as invalid, because this latest set / delete
		// operation supersedes all older values.
		for i := len(c.db.store[key]) - 1; i >= 0; i-- {
			value := &c.db.store[key][i]
			debug(value, c.tx, c.db.isVisible(c.tx, *value))
			if c.db.isVisible(c.tx, *value) {
				value.txEndID = c.tx.id
				found = true
			}
		}

		if command == "delete" && !found {
			return "", fmt.Errorf("delete failed: key %q not found")
		}

		// We track the key being written.
		c.tx.writeset.Insert(key)

		if command == "set" {
			value := args[1]
			c.db.store[key] = append(c.db.store[key], Value{
				txStartID: c.tx.id,
				// end ID '0' implies no valid transaction has overwritten this value yet.
				txEndID: 0,
				value:   value,
			})

			return value, nil
		}

		// DELETE operation successful, as we have already marked all existing values as invalid (by
		// setting a Tx ID to `txEndID`).
		return "", nil

	}

	return "", fmt.Errorf("command unimplemented")
}

// TODO: Why do we need mustExecCommand and execCommand. Why not just execCommand?
func (c *Connection) mustExecCommand(cmd string, args []string) string {
	res, err := c.execCommand(cmd, args)
	assertEq(err, nil, "unexpected error")
	return res
}

func (d *Database) newConnection() *Connection {
	return &Connection{
		db: d,
		tx: nil,
	}
}

func main() {
	panic("unimplemented")
}

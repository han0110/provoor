package cluster

import "time"

// DefaultProveTimeout bounds one proof.
const DefaultProveTimeout = 10 * time.Minute

// ProveOutcome is what one completed proof reports.
type ProveOutcome struct {
	// PublicValues is the fixed-size commitment the guest produced.
	PublicValues []byte
	// ProofBytes is the size of the proof envelope.
	ProofBytes int
	// ClusterProvingTime is the proving duration the cluster reports.
	ClusterProvingTime time.Duration
	// SubmitWait is the time spent on submissions the cluster refused before
	// it admitted one. A coordinator reports itself ready while it declines
	// work, so the measured time excludes this.
	SubmitWait time.Duration
	// Pipeline is the task timeline of the proof, nil on a zkVM that reports
	// none.
	Pipeline *Pipeline
}

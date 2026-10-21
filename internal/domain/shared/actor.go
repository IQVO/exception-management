package shared

// ActorKind says WHO performed a transition: a human operator, a declared
// policy (its derived rule id) or the system itself (a consumed fact, the
// sweeper). Every audit entry and every domain event records one (ADR 0002).
type ActorKind string

// The three actor kinds.
const (
	ActorSystem   ActorKind = "system"
	ActorOperator ActorKind = "operator"
	ActorPolicy   ActorKind = "policy"
)

// Actor is the recorded author of a transition. Name is what the wire
// carries (the operator id, the derived rule id, or "system"); Kind is the
// domain's own classification.
type Actor struct {
	Kind ActorKind
	Name string
}

// System returns the system actor.
func System() Actor { return Actor{Kind: ActorSystem, Name: "system"} }

// Operator validates and returns an operator actor.
func Operator(name string) (Actor, error) {
	if !ValidText(name) {
		return Actor{}, ErrBlankActor
	}
	return Actor{Kind: ActorOperator, Name: name}, nil
}

// Policy validates and returns a policy actor (named by its derived rule id).
func Policy(name string) (Actor, error) {
	if !ValidText(name) {
		return Actor{}, ErrBlankActor
	}
	return Actor{Kind: ActorPolicy, Name: name}, nil
}

// Valid reports whether the actor can be recorded (non-blank, no NUL).
func (a Actor) Valid() bool {
	switch a.Kind {
	case ActorSystem:
		return a.Name == "system"
	case ActorOperator, ActorPolicy:
		return ValidText(a.Name)
	default:
		return false
	}
}

// String returns the wire form of the actor: its name.
func (a Actor) String() string { return a.Name }

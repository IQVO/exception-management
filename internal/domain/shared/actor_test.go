package shared

import "testing"

func TestSystemOperatorAndPolicyConstructorsBuildValidActors(t *testing.T) {
	t.Parallel()
	sys := System()
	if !sys.Valid() || sys.Kind != ActorSystem || sys.String() != "system" {
		t.Fatalf("System() = %+v", sys)
	}
	op, err := Operator("op-jane")
	if err != nil || !op.Valid() || op.Kind != ActorOperator || op.String() != "op-jane" {
		t.Fatalf("Operator(op-jane) = %+v, %v", op, err)
	}
	pol, err := Policy("rule-1")
	if err != nil || !pol.Valid() || pol.Kind != ActorPolicy || pol.String() != "rule-1" {
		t.Fatalf("Policy(rule-1) = %+v, %v", pol, err)
	}
}

func TestActorConstructorsRejectBlankNames(t *testing.T) {
	t.Parallel()
	bad := []string{"", " ", "\x00"}
	for _, name := range bad {
		if _, err := Operator(name); err != ErrBlankActor {
			t.Errorf("Operator(%q) err = %v, want ErrBlankActor", name, err)
		}
		if _, err := Policy(name); err != ErrBlankActor {
			t.Errorf("Policy(%q) err = %v, want ErrBlankActor", name, err)
		}
	}
}

func TestInvalidActorStructLiteralsFailValidation(t *testing.T) {
	t.Parallel()
	invalid := []Actor{
		{Kind: ActorSystem, Name: "op-jane"},
		{Kind: ActorOperator, Name: ""},
		{Kind: ActorOperator, Name: " "},
		{Kind: ActorPolicy, Name: "\x00"},
		{Kind: ActorKind("ghost"), Name: "x"},
		{},
	}
	for _, a := range invalid {
		if a.Valid() {
			t.Errorf("%+v must be invalid", a)
		}
	}
}

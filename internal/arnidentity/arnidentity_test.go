package arnidentity

import "testing"

func TestParseAndResourceName(t *testing.T) {
	id, err := Parse("arn:aws:events:eu-west-1:123456789012:event-bus/orders")
	if err != nil {
		t.Fatal(err)
	}
	if id.Service != "events" || id.Region != "eu-west-1" || id.Account != "123456789012" {
		t.Fatalf("identity = %#v", id)
	}
	if got, ok := id.ResourceName("event-bus/"); !ok || got != "orders" {
		t.Fatalf("resource name = %q, %v", got, ok)
	}
}

func TestParseRejectsMalformedARN(t *testing.T) {
	if _, err := Parse("not-an-arn"); err == nil {
		t.Fatal("malformed ARN accepted")
	}
}

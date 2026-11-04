package runner

import "testing"

func TestParseCluster(t *testing.T) {
	got, err := ParseCluster("0=127.0.0.1:7000/127.0.0.1:8000, 1=h1:7001/h1:8001")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Raft != "h1:7001" || got[0].HTTP != "127.0.0.1:8000" {
		t.Fatalf("got %+v", got)
	}
}

func TestParseClusterRejectsBadInput(t *testing.T) {
	for _, spec := range []string{
		"",
		"0=host:7000",         // no http address
		"x=a:1/b:2",           // id is not a number
		"0=a:1/b:2,0=c:3/d:4", // duplicate
		"=a:1/b:2",            // no id
		"0=/b:2",              // empty raft address
	} {
		if _, err := ParseCluster(spec); err == nil {
			t.Errorf("ParseCluster(%q) succeeded, want an error", spec)
		}
	}
}

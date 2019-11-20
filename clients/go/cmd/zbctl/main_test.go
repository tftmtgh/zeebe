package main

import (
	"context"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/zeebe-io/zeebe/clients/go/internal/containerSuite"
	"io/ioutil"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

var zbctl string

type integrationTestSuite struct {
	*containerSuite.ContainerSuite
}

var tests = []struct {
	name       string
	setupCmds  []string
	cmd        string
	goldenFile string
}{
	{
		name:       "print help",
		cmd:        "help",
		goldenFile: "testdata/help.golden",
	},
	// TODO: doesn't always print 'Partition 1 : Leader'
	//{
	//	command:    "status --insecure",
	//	goldenFile: "testdata/status.golden",
	//},
	{
		name:       "get topology",
		cmd:        "status",
		goldenFile: "testdata/without_insecure.golden",
	},
	{
		name:       "deploy workflow",
		cmd:        "--insecure deploy testdata/model.bpmn",
		goldenFile: "testdata/deploy.golden",
	},
	{
		name:       "create instance",
		setupCmds:  []string{"--insecure deploy testdata/model.bpmn"},
		cmd:        "--insecure create instance process",
		goldenFile: "testdata/create_instance.golden",
	},
	{
		name:       "create worker",
		setupCmds:  []string{"--insecure deploy testdata/job_model.bpmn", "--insecure create instance jobProcess"},
		cmd:        "create --insecure worker jobType --handler echo",
		goldenFile: "testdata/create_worker.golden",
	},
	{
		name:       "activate job",
		setupCmds:  []string{"--insecure deploy testdata/job_model.bpmn", "--insecure create instance jobProcess"},
		cmd:        "--insecure activate jobs jobType",
		goldenFile: "testdata/activate_job.golden",
	},
}

func TestZbctlSuite(t *testing.T) {
	err := buildZbctl()
	if err != nil {
		t.Fatal(errors.Wrap(err, "couldn't build zbctl"))
	}

	suite.Run(t,
		&integrationTestSuite{
			ContainerSuite: &containerSuite.ContainerSuite{
				Timeout:        time.Second,
				ContainerImage: "camunda/zeebe:current-test",
			},
		})
}

func (s *integrationTestSuite) TestSuiteCases() {
	for _, test := range tests {
		s.T().Run(test.name, func(t *testing.T) {
			for _, cmd := range test.setupCmds {
				if _, err := s.runCommand(cmd); err != nil {
					t.Fatal(errors.Wrap(err, "failed while executing set up command '%"))
				}
			}

			cmdOut, _ := s.runCommand(test.cmd)

			goldenOut, err := ioutil.ReadFile(test.goldenFile)
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Split(string(goldenOut), "\n")
			got := strings.Split(string(cmdOut), "\n")

			if diff := cmp.Diff(want, got, cmp.Comparer(compareStrIgnoreNumeric)); diff != "" {
				s.T().Fatalf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func compareStrIgnoreNumeric(x, y string) bool {
	reg, err := regexp.Compile("\\d")
	if err != nil {
		panic(err)
	}

	newX := reg.ReplaceAllString(x, "")
	newY := reg.ReplaceAllString(y, "")

	return newX == newY
}

// runCommand runs the zbctl command and returns the combined output from stdout and stderr
func (s *integrationTestSuite) runCommand(command string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	args := append(strings.Fields(command), "--address", s.GatewayAddress)
	cmd := exec.CommandContext(ctx, fmt.Sprintf("./dist/%s", zbctl), args...)

	return cmd.CombinedOutput()
}

func buildZbctl() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if runtime.GOOS == "linux" {
		zbctl = "zbctl"
	} else if runtime.GOOS == "windows" {
		zbctl = "zbctl.exe"
	} else if runtime.GOOS == "darwin" {
		zbctl = "zbctl.darwin"
	} else {
		return errors.Errorf("Can't run zbctl tests on unsupported OS '%s'", runtime.GOOS)
	}

	return exec.CommandContext(ctx, "./build.sh", runtime.GOOS).Run()
}

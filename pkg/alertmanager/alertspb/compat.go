// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertspb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertspb

import "errors"

var (
	ErrNotFound = errors.New("alertmanager storage object not found")
)

// ToProto transforms a yaml Alertmanager config and map of template files to an AlertConfigDesc.
func ToProto(cfg string, templates map[string]string, user string) AlertConfigDesc {
	tmpls := []*TemplateDesc{}
	for fn, body := range templates {
		tmpls = append(tmpls, &TemplateDesc{
			Body:     body,
			Filename: fn,
		})
	}
	return AlertConfigDesc{
		User:      user,
		RawConfig: cfg,
		Templates: tmpls,
	}
}

// ToGrafanaProto transforms a Grafana Alertmanager config to a GrafanaAlertConfigDesc.
func ToGrafanaProto(cfg, user, hash string, id int64, createdAtTimestamp int64, isDefault bool) GrafanaAlertConfigDesc {
	return GrafanaAlertConfigDesc{
		User:               user,
		RawConfig:          cfg,
		Id:                 id,
		Hash:               hash,
		CreatedAtTimestamp: createdAtTimestamp,
		Default:            isDefault,
	}
}

// ParseTemplates returns an Alertmanager config object.
func ParseTemplates(cfg AlertConfigDesc) map[string]string {
	templates := map[string]string{}
	for _, t := range cfg.Templates {
		templates[t.Filename] = t.Body
	}
	return templates
}

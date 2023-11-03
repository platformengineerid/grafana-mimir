package alertmanager

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/cluster/clusterpb"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type UserGrafanaConfig struct {
	TemplateFiles             map[string]string `json:"template_files"`
	GrafanaAlertmanagerConfig string            `json:"grafana_alertmanager_config"`
}

type UserGrafanaState struct {
	State string `json:"state"`
}

func (am *MultitenantAlertmanager) GetUserGrafanaState(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	st, err := am.store.GetFullGrafanaState(r.Context(), userID)
	if err != nil {
		if errors.Is(err, alertspb.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	bytes, err := st.State.Marshal()
	if err != nil {
		//TODO: constants?
		level.Error(logger).Log("msg", "error marshalling state", "err", err, "user", userID)
		http.Error(w, fmt.Sprintf("%s: %s", "error marshalling state", err.Error()), http.StatusInternalServerError)
	}

	d, err := json.Marshal(&UserGrafanaState{
		State: base64.StdEncoding.EncodeToString(bytes),
	})

	if err != nil {
		level.Error(logger).Log("msg", errMarshallingJSON, "err", err, "user", userID)
		http.Error(w, fmt.Sprintf("%s: %s", errMarshallingJSON, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(d); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (am *MultitenantAlertmanager) SetUserGrafanaState(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	//TODO: Limits?
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		level.Error(logger).Log("msg", "unable to read grafana state", "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", "unable to read grafana state", err.Error()), http.StatusBadRequest)
		return
	}

	st := &UserGrafanaState{}
	err = json.Unmarshal(payload, st)
	if err != nil {
		level.Error(logger).Log("msg", errMarshallingJSON, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errMarshallingJSON, err.Error()), http.StatusBadRequest)
		return
	}

	// TODO: Validation or limits.

	decodedBytes, err := base64.StdEncoding.DecodeString(st.State)
	if err != nil {
		level.Error(logger).Log("msg", "unable to base64 decode state", "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", "unable to base64 decode state", err.Error()), http.StatusBadRequest)
		return
	}

	var protoState *clusterpb.FullState
	err = protoState.Unmarshal(decodedBytes)
	if err != nil {
		level.Error(logger).Log("msg", "unable to decode proto for state", "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", "unable to decode proto for state", err.Error()), http.StatusInternalServerError)
		return
	}

	err = am.store.SetFullGrafanaState(r.Context(), userID, alertspb.FullStateDesc{State: protoState})
	if err != nil {
		level.Error(logger).Log("msg", errStoringConfiguration, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errStoringConfiguration, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
func (am *MultitenantAlertmanager) DeleteUserGrafanaState(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	err = am.store.DeleteFullGrafanaState(r.Context(), userID)
	if err != nil {
		level.Error(logger).Log("msg", "unable to delete the grafana state", "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", "unable to delete the grafana state", err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)

}

func (am *MultitenantAlertmanager) GetUserGrafanaConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	cfg, err := am.store.GetGrafanaAlertConfig(r.Context(), userID)
	if err != nil {
		if errors.Is(err, alertspb.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	d, err := json.Marshal(&UserGrafanaConfig{
		TemplateFiles:             alertspb.ParseGrafanaTemplates(cfg),
		GrafanaAlertmanagerConfig: cfg.RawConfig,
	})

	if err != nil {
		level.Error(logger).Log("msg", errMarshallingJSON, "err", err, "user", userID)
		http.Error(w, fmt.Sprintf("%s: %s", errMarshallingJSON, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(d); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (am *MultitenantAlertmanager) SetUserGrafanaConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	var input io.Reader
	maxConfigSize := am.limits.AlertmanagerMaxConfigSize(userID)
	if maxConfigSize > 0 {
		// LimitReader will return EOF after reading specified number of bytes. To check if
		// we have read too many bytes, allow one extra byte.
		input = io.LimitReader(r.Body, int64(maxConfigSize)+1)
	} else {
		input = r.Body
	}

	payload, err := io.ReadAll(input)
	if err != nil {
		level.Error(logger).Log("msg", errReadingConfiguration, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errReadingConfiguration, err.Error()), http.StatusBadRequest)
		return
	}

	if maxConfigSize > 0 && len(payload) > maxConfigSize {
		msg := fmt.Sprintf(errConfigurationTooBig, maxConfigSize)
		level.Warn(logger).Log("msg", msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	cfg := &UserGrafanaConfig{}
	err = json.Unmarshal(payload, cfg)
	if err != nil {
		level.Error(logger).Log("msg", errMarshallingJSON, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errMarshallingJSON, err.Error()), http.StatusBadRequest)
		return
	}

	cfgDesc := alertspb.ToGrafanaProto(cfg.GrafanaAlertmanagerConfig, cfg.TemplateFiles, userID)
	//cfgDesc := alertspb.ToProto(cfg.AlertmanagerConfig, cfg.TemplateFiles, userID)
	//if err := validateUserConfig(logger, cfgDesc, am.limits, userID); err != nil {
	//	level.Warn(logger).Log("msg", errValidatingConfig, "err", err.Error())
	//	http.Error(w, fmt.Sprintf("%s: %s", errValidatingConfig, err.Error()), http.StatusBadRequest)
	//	return
	//}

	err = am.store.SetGrafanaAlertConfig(r.Context(), cfgDesc)
	if err != nil {
		level.Error(logger).Log("msg", errStoringConfiguration, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errStoringConfiguration, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// DeleteUserConfig is exposed via user-visible API (if enabled, uses DELETE method), but also as an internal endpoint using POST method.
// Note that if no config exists for a user, StatusOK is returned.
func (am *MultitenantAlertmanager) DeleteUserGrafanaConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	err = am.store.DeleteGrafanaAlertConfig(r.Context(), userID)
	if err != nil {
		level.Error(logger).Log("msg", errDeletingConfiguration, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errDeletingConfiguration, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

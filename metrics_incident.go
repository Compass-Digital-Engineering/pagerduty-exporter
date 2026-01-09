package main

import (
	"strings"
	"time"

	"github.com/PagerDuty/go-pagerduty"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/webdevops/go-common/prometheus/collector"
)

type MetricsCollectorIncident struct {
	collector.Processor

	prometheus struct {
		incident       *prometheus.GaugeVec
		incidentStatus *prometheus.GaugeVec
		incidentMTTA   *prometheus.GaugeVec
		serviceMTTA    *prometheus.GaugeVec
		incidentMTTR   *prometheus.GaugeVec
		serviceMTTR    *prometheus.GaugeVec
		// P1-specific metrics
		incidentMTTAP1 *prometheus.GaugeVec
		serviceMTTAP1  *prometheus.GaugeVec
		incidentMTTRP1 *prometheus.GaugeVec
		serviceMTTRP1  *prometheus.GaugeVec
		// P2-specific metrics
		incidentMTTAP2 *prometheus.GaugeVec
		serviceMTTAP2  *prometheus.GaugeVec
		incidentMTTRP2 *prometheus.GaugeVec
		serviceMTTRP2  *prometheus.GaugeVec
		// Corporate HQ metrics
		incidentMTTACorporateHQ *prometheus.GaugeVec
		serviceMTTACorporateHQ  *prometheus.GaugeVec
		incidentMTTRCorporateHQ *prometheus.GaugeVec
		serviceMTTRCorporateHQ  *prometheus.GaugeVec
		// P1 without Corporate HQ metrics
		incidentMTTAP1NoCorporateHQ *prometheus.GaugeVec
		serviceMTTAP1NoCorporateHQ  *prometheus.GaugeVec
		incidentMTTRP1NoCorporateHQ *prometheus.GaugeVec
		serviceMTTRP1NoCorporateHQ  *prometheus.GaugeVec
	}

	teamListOpt []string
}

func (m *MetricsCollectorIncident) Setup(collector *collector.Collector) {
	m.Processor.Setup(collector)

	m.prometheus.incident = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_info",
			Help: "PagerDuty incident",
		},
		[]string{
			"incidentID",
			"serviceID",
			"incidentUrl",
			"incidentNumber",
			"title",
			"status",
			"urgency",
			"acknowledged",
			"assigned",
			"type",
			"time",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_info", m.prometheus.incident, true)

	m.prometheus.incidentStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_status",
			Help: "PagerDuty incident status",
		},
		[]string{
			"incidentID",
			"userID",
			"time",
			"type",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_status", m.prometheus.incidentStatus, true)

	m.prometheus.incidentMTTA = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mtta_seconds",
			Help: "PagerDuty incident Mean Time To Acknowledgment in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"acknowledgerID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mtta_seconds", m.prometheus.incidentMTTA, true)

	m.prometheus.serviceMTTA = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mtta_seconds",
			Help: "PagerDuty service-level Mean Time To Acknowledgment in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mtta_seconds", m.prometheus.serviceMTTA, true)

	m.prometheus.incidentMTTR = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mttr_seconds",
			Help: "PagerDuty incident Mean Time To Resolution in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"resolverID",
			"priority",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mttr_seconds", m.prometheus.incidentMTTR, true)

	m.prometheus.serviceMTTR = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mttr_seconds",
			Help: "PagerDuty service-level Mean Time To Resolution in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
			"priority",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mttr_seconds", m.prometheus.serviceMTTR, true)

	// P1-specific MTTA metrics
	m.prometheus.incidentMTTAP1 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mtta_seconds_p1",
			Help: "PagerDuty P1 incident Mean Time To Acknowledgment in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"acknowledgerID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mtta_seconds_p1", m.prometheus.incidentMTTAP1, true)

	m.prometheus.serviceMTTAP1 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mtta_seconds_p1",
			Help: "PagerDuty service-level P1 Mean Time To Acknowledgment in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mtta_seconds_p1", m.prometheus.serviceMTTAP1, true)

	// P1-specific MTTR metrics
	m.prometheus.incidentMTTRP1 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mttr_seconds_p1",
			Help: "PagerDuty P1 incident Mean Time To Resolution in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"resolverID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mttr_seconds_p1", m.prometheus.incidentMTTRP1, true)

	m.prometheus.serviceMTTRP1 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mttr_seconds_p1",
			Help: "PagerDuty service-level P1 Mean Time To Resolution in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mttr_seconds_p1", m.prometheus.serviceMTTRP1, true)

	// P2-specific MTTA metrics
	m.prometheus.incidentMTTAP2 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mtta_seconds_p2",
			Help: "PagerDuty P2 incident Mean Time To Acknowledgment in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"acknowledgerID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mtta_seconds_p2", m.prometheus.incidentMTTAP2, true)

	m.prometheus.serviceMTTAP2 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mtta_seconds_p2",
			Help: "PagerDuty service-level P2 Mean Time To Acknowledgment in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mtta_seconds_p2", m.prometheus.serviceMTTAP2, true)

	// P2-specific MTTR metrics
	m.prometheus.incidentMTTRP2 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mttr_seconds_p2",
			Help: "PagerDuty P2 incident Mean Time To Resolution in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"resolverID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mttr_seconds_p2", m.prometheus.incidentMTTRP2, true)

	m.prometheus.serviceMTTRP2 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mttr_seconds_p2",
			Help: "PagerDuty service-level P2 Mean Time To Resolution in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mttr_seconds_p2", m.prometheus.serviceMTTRP2, true)

	// Corporate HQ MTTA metrics
	m.prometheus.incidentMTTACorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mtta_seconds_corporatehq",
			Help: "PagerDuty Corporate HQ incident Mean Time To Acknowledgment in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"acknowledgerID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mtta_seconds_corporatehq", m.prometheus.incidentMTTACorporateHQ, true)

	m.prometheus.serviceMTTACorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mtta_seconds_corporatehq",
			Help: "PagerDuty service-level Corporate HQ Mean Time To Acknowledgment in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mtta_seconds_corporatehq", m.prometheus.serviceMTTACorporateHQ, true)

	// Corporate HQ MTTR metrics
	m.prometheus.incidentMTTRCorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mttr_seconds_corporatehq",
			Help: "PagerDuty Corporate HQ incident Mean Time To Resolution in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"resolverID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mttr_seconds_corporatehq", m.prometheus.incidentMTTRCorporateHQ, true)

	m.prometheus.serviceMTTRCorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mttr_seconds_corporatehq",
			Help: "PagerDuty service-level Corporate HQ Mean Time To Resolution in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mttr_seconds_corporatehq", m.prometheus.serviceMTTRCorporateHQ, true)

	// P1 without Corporate HQ MTTA metrics
	m.prometheus.incidentMTTAP1NoCorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mtta_seconds_p1_no_corporatehq",
			Help: "PagerDuty P1 (excluding Corporate HQ) incident Mean Time To Acknowledgment in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"acknowledgerID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mtta_seconds_p1_no_corporatehq", m.prometheus.incidentMTTAP1NoCorporateHQ, true)

	m.prometheus.serviceMTTAP1NoCorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mtta_seconds_p1_no_corporatehq",
			Help: "PagerDuty service-level P1 (excluding Corporate HQ) Mean Time To Acknowledgment in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mtta_seconds_p1_no_corporatehq", m.prometheus.serviceMTTAP1NoCorporateHQ, true)

	// P1 without Corporate HQ MTTR metrics
	m.prometheus.incidentMTTRP1NoCorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_incident_mttr_seconds_p1_no_corporatehq",
			Help: "PagerDuty P1 (excluding Corporate HQ) incident Mean Time To Resolution in seconds",
		},
		[]string{
			"incidentID",
			"serviceID",
			"serviceName",
			"urgency",
			"resolverID",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_incident_mttr_seconds_p1_no_corporatehq", m.prometheus.incidentMTTRP1NoCorporateHQ, true)

	m.prometheus.serviceMTTRP1NoCorporateHQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pagerduty_service_mttr_seconds_p1_no_corporatehq",
			Help: "PagerDuty service-level P1 (excluding Corporate HQ) Mean Time To Resolution in seconds (rolling average)",
		},
		[]string{
			"serviceID",
			"serviceName",
			"urgency",
		},
	)
	m.Collector.RegisterMetricList("pagerduty_service_mttr_seconds_p1_no_corporatehq", m.prometheus.serviceMTTRP1NoCorporateHQ, true)
}

func (m *MetricsCollectorIncident) Reset() {
}

func (m *MetricsCollectorIncident) Collect(callback chan<- func()) {
	listOpts := pagerduty.ListIncidentsOptions{
		Includes: []string{"acknowledgers"},
	}
	listOpts.Limit = PagerdutyListLimit
	listOpts.Statuses = Opts.PagerDuty.Incident.Statuses
	listOpts.Offset = 0
	listOpts.SortBy = "created_at:desc"

	if len(m.teamListOpt) > 0 {
		listOpts.TeamIDs = m.teamListOpt
	}

	// Check if resolved incidents are already included in configured statuses
	includesResolved := false
	for _, status := range Opts.PagerDuty.Incident.Statuses {
		if status == "resolved" || status == "all" {
			includesResolved = true
			break
		}
	}

	// Fetch resolved incidents separately for MTTR/MTTA if not already included
	var resolvedIncidents []pagerduty.Incident
	if !includesResolved {
		resolvedOpts := pagerduty.ListIncidentsOptions{
			Includes: []string{"acknowledgers"},
		}
		resolvedOpts.Limit = PagerdutyListLimit
		resolvedOpts.Statuses = []string{"resolved"}
		resolvedOpts.Offset = 0
		resolvedOpts.SortBy = "created_at:desc"

		if len(m.teamListOpt) > 0 {
			resolvedOpts.TeamIDs = m.teamListOpt
		}

		for {
			resolvedList, err := PagerDutyClient.ListIncidentsWithContext(m.Context(), resolvedOpts)
			PrometheusPagerDutyApiCounter.WithLabelValues("ListIncidents").Inc()

			if err != nil {
				m.Logger().Panic(err)
			}

			resolvedIncidents = append(resolvedIncidents, resolvedList.Incidents...)

			resolvedOpts.Offset += resolvedOpts.Limit
			if stopPagerdutyPaging(resolvedList.APIListObject) || resolvedOpts.Offset >= Opts.PagerDuty.Incident.Limit {
				break
			}
		}
	}

	incidentMetricList := m.Collector.GetMetricList("pagerduty_incident_info")
	incidentStatusMetricList := m.Collector.GetMetricList("pagerduty_incident_status")
	incidentMTTAMetricList := m.Collector.GetMetricList("pagerduty_incident_mtta_seconds")
	serviceMTTAMetricList := m.Collector.GetMetricList("pagerduty_service_mtta_seconds")
	incidentMTTRMetricList := m.Collector.GetMetricList("pagerduty_incident_mttr_seconds")
	serviceMTTRMetricList := m.Collector.GetMetricList("pagerduty_service_mttr_seconds")

	// P1-specific metric lists
	incidentMTTAP1MetricList := m.Collector.GetMetricList("pagerduty_incident_mtta_seconds_p1")
	serviceMTTAP1MetricList := m.Collector.GetMetricList("pagerduty_service_mtta_seconds_p1")
	incidentMTTRP1MetricList := m.Collector.GetMetricList("pagerduty_incident_mttr_seconds_p1")
	serviceMTTRP1MetricList := m.Collector.GetMetricList("pagerduty_service_mttr_seconds_p1")

	// P2-specific metric lists
	incidentMTTAP2MetricList := m.Collector.GetMetricList("pagerduty_incident_mtta_seconds_p2")
	serviceMTTAP2MetricList := m.Collector.GetMetricList("pagerduty_service_mtta_seconds_p2")
	incidentMTTRP2MetricList := m.Collector.GetMetricList("pagerduty_incident_mttr_seconds_p2")
	serviceMTTRP2MetricList := m.Collector.GetMetricList("pagerduty_service_mttr_seconds_p2")

	// Corporate HQ metric lists
	incidentMTTACorporateHQMetricList := m.Collector.GetMetricList("pagerduty_incident_mtta_seconds_corporatehq")
	serviceMTTACorporateHQMetricList := m.Collector.GetMetricList("pagerduty_service_mtta_seconds_corporatehq")
	incidentMTTRCorporateHQMetricList := m.Collector.GetMetricList("pagerduty_incident_mttr_seconds_corporatehq")
	serviceMTTRCorporateHQMetricList := m.Collector.GetMetricList("pagerduty_service_mttr_seconds_corporatehq")

	// P1 without Corporate HQ metric lists
	incidentMTTAP1NoCorporateHQMetricList := m.Collector.GetMetricList("pagerduty_incident_mtta_seconds_p1_no_corporatehq")
	serviceMTTAP1NoCorporateHQMetricList := m.Collector.GetMetricList("pagerduty_service_mtta_seconds_p1_no_corporatehq")
	incidentMTTRP1NoCorporateHQMetricList := m.Collector.GetMetricList("pagerduty_incident_mttr_seconds_p1_no_corporatehq")
	serviceMTTRP1NoCorporateHQMetricList := m.Collector.GetMetricList("pagerduty_service_mttr_seconds_p1_no_corporatehq")

	// Track MTTA/MTTR data per service for calculating averages
	serviceMTTAData := make(map[string][]float64) // key: serviceID_urgency
	serviceMTTRData := make(map[string][]float64) // key: serviceID|urgency|priority
	serviceNames := make(map[string]string)       // cache serviceID -> serviceName

	// P1-specific tracking
	serviceMTTAP1Data := make(map[string][]float64) // key: serviceID_urgency
	serviceMTTRP1Data := make(map[string][]float64) // key: serviceID_urgency

	// P2-specific tracking
	serviceMTTAP2Data := make(map[string][]float64) // key: serviceID_urgency
	serviceMTTRP2Data := make(map[string][]float64) // key: serviceID_urgency

	// Corporate HQ tracking
	serviceMTTACorporateHQData := make(map[string][]float64) // key: serviceID_urgency
	serviceMTTRCorporateHQData := make(map[string][]float64) // key: serviceID_urgency

	// P1 without Corporate HQ tracking
	serviceMTTAP1NoCorporateHQData := make(map[string][]float64) // key: serviceID_urgency
	serviceMTTRP1NoCorporateHQData := make(map[string][]float64) // key: serviceID_urgency

	for {
		m.Logger().Debugf("fetch incidents (offset: %v, limit:%v)", listOpts.Offset, listOpts.Limit)

		list, err := PagerDutyClient.ListIncidentsWithContext(m.Context(), listOpts)
		PrometheusPagerDutyApiCounter.WithLabelValues("ListIncidents").Inc()

		if err != nil {
			m.Logger().Panic(err)
		}

		for _, incident := range list.Incidents {
			m.processIncident(incident, incidentMetricList, incidentStatusMetricList,
				incidentMTTAMetricList, incidentMTTRMetricList,
				serviceMTTAData, serviceMTTRData, serviceNames,
				incidentMTTAP1MetricList, incidentMTTRP1MetricList, serviceMTTAP1Data, serviceMTTRP1Data,
				incidentMTTAP2MetricList, incidentMTTRP2MetricList, serviceMTTAP2Data, serviceMTTRP2Data,
				incidentMTTACorporateHQMetricList, incidentMTTRCorporateHQMetricList, serviceMTTACorporateHQData, serviceMTTRCorporateHQData,
				incidentMTTAP1NoCorporateHQMetricList, incidentMTTRP1NoCorporateHQMetricList, serviceMTTAP1NoCorporateHQData, serviceMTTRP1NoCorporateHQData)
		}

		listOpts.Offset += PagerdutyListLimit
		if stopPagerdutyPaging(list.APIListObject) || listOpts.Offset >= Opts.PagerDuty.Incident.Limit {
			break
		}
	}

	// Process resolved incidents if fetched separately
	for _, incident := range resolvedIncidents {
		m.processIncident(incident, incidentMetricList, incidentStatusMetricList,
			incidentMTTAMetricList, incidentMTTRMetricList,
			serviceMTTAData, serviceMTTRData, serviceNames,
			incidentMTTAP1MetricList, incidentMTTRP1MetricList, serviceMTTAP1Data, serviceMTTRP1Data,
			incidentMTTAP2MetricList, incidentMTTRP2MetricList, serviceMTTAP2Data, serviceMTTRP2Data,
			incidentMTTACorporateHQMetricList, incidentMTTRCorporateHQMetricList, serviceMTTACorporateHQData, serviceMTTRCorporateHQData,
			incidentMTTAP1NoCorporateHQMetricList, incidentMTTRP1NoCorporateHQMetricList, serviceMTTAP1NoCorporateHQData, serviceMTTRP1NoCorporateHQData)
	}

	// Calculate and set service-level MTTA averages
	for serviceKey, mttaValues := range serviceMTTAData {
		if len(mttaValues) == 0 {
			continue
		}

		// Parse serviceKey (format: serviceID_urgency)
		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mtta := range mttaValues {
			total += mtta
		}
		avgMTTA := total / float64(len(mttaValues))

		serviceMTTAMetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTA)
	}

	// Calculate and set service-level MTTR averages
	for serviceKey, mttrValues := range serviceMTTRData {
		if len(mttrValues) == 0 {
			continue
		}

		// Parse serviceKey (format: serviceID|urgency|priority)
		parts := strings.Split(serviceKey, "|")
		if len(parts) != 3 {
			continue
		}
		serviceID, urgency, priority := parts[0], parts[1], parts[2]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mttr := range mttrValues {
			total += mttr
		}
		avgMTTR := total / float64(len(mttrValues))

		serviceMTTRMetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
			"priority":    priority,
		}, avgMTTR)
	}

	// Calculate and set service-level P1 MTTA averages
	for serviceKey, mttaValues := range serviceMTTAP1Data {
		if len(mttaValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mtta := range mttaValues {
			total += mtta
		}
		avgMTTA := total / float64(len(mttaValues))

		serviceMTTAP1MetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTA)
	}

	// Calculate and set service-level P1 MTTR averages
	for serviceKey, mttrValues := range serviceMTTRP1Data {
		if len(mttrValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mttr := range mttrValues {
			total += mttr
		}
		avgMTTR := total / float64(len(mttrValues))

		serviceMTTRP1MetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTR)
	}

	// Calculate and set service-level P2 MTTA averages
	for serviceKey, mttaValues := range serviceMTTAP2Data {
		if len(mttaValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mtta := range mttaValues {
			total += mtta
		}
		avgMTTA := total / float64(len(mttaValues))

		serviceMTTAP2MetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTA)
	}

	// Calculate and set service-level P2 MTTR averages
	for serviceKey, mttrValues := range serviceMTTRP2Data {
		if len(mttrValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mttr := range mttrValues {
			total += mttr
		}
		avgMTTR := total / float64(len(mttrValues))

		serviceMTTRP2MetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTR)
	}

	// Calculate and set service-level Corporate HQ MTTA averages
	for serviceKey, mttaValues := range serviceMTTACorporateHQData {
		if len(mttaValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mtta := range mttaValues {
			total += mtta
		}
		avgMTTA := total / float64(len(mttaValues))

		serviceMTTACorporateHQMetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTA)
	}

	// Calculate and set service-level Corporate HQ MTTR averages
	for serviceKey, mttrValues := range serviceMTTRCorporateHQData {
		if len(mttrValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mttr := range mttrValues {
			total += mttr
		}
		avgMTTR := total / float64(len(mttrValues))

		serviceMTTRCorporateHQMetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTR)
	}

	// Calculate and set service-level P1 without Corporate HQ MTTA averages
	for serviceKey, mttaValues := range serviceMTTAP1NoCorporateHQData {
		if len(mttaValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mtta := range mttaValues {
			total += mtta
		}
		avgMTTA := total / float64(len(mttaValues))

		serviceMTTAP1NoCorporateHQMetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTA)
	}

	// Calculate and set service-level P1 without Corporate HQ MTTR averages
	for serviceKey, mttrValues := range serviceMTTRP1NoCorporateHQData {
		if len(mttrValues) == 0 {
			continue
		}

		lastUnderscore := strings.LastIndex(serviceKey, "_")
		if lastUnderscore == -1 {
			continue
		}
		serviceID := serviceKey[:lastUnderscore]
		urgency := serviceKey[lastUnderscore+1:]

		serviceName := m.getCachedServiceName(serviceID, serviceNames)

		var total float64
		for _, mttr := range mttrValues {
			total += mttr
		}
		avgMTTR := total / float64(len(mttrValues))

		serviceMTTRP1NoCorporateHQMetricList.Add(prometheus.Labels{
			"serviceID":   serviceID,
			"serviceName": serviceName,
			"urgency":     urgency,
		}, avgMTTR)
	}
}

func (m *MetricsCollectorIncident) processIncident(
	incident pagerduty.Incident,
	incidentMetricList, incidentStatusMetricList, incidentMTTAMetricList, incidentMTTRMetricList *collector.MetricList,
	serviceMTTAData, serviceMTTRData map[string][]float64,
	serviceNames map[string]string,
	incidentMTTAP1MetricList, incidentMTTRP1MetricList *collector.MetricList,
	serviceMTTAP1Data, serviceMTTRP1Data map[string][]float64,
	incidentMTTAP2MetricList, incidentMTTRP2MetricList *collector.MetricList,
	serviceMTTAP2Data, serviceMTTRP2Data map[string][]float64,
	incidentMTTACorporateHQMetricList, incidentMTTRCorporateHQMetricList *collector.MetricList,
	serviceMTTACorporateHQData, serviceMTTRCorporateHQData map[string][]float64,
	incidentMTTAP1NoCorporateHQMetricList, incidentMTTRP1NoCorporateHQMetricList *collector.MetricList,
	serviceMTTAP1NoCorporateHQData, serviceMTTRP1NoCorporateHQData map[string][]float64,
) {
	createdAt, _ := time.Parse(time.RFC3339, incident.CreatedAt)

	// Apply filtering for CDCE services, non-DT alerts, and P1/P2 priority
	shouldReport := m.shouldReportIncident(incident)

	if shouldReport {
		incidentMetricList.AddTime(prometheus.Labels{
			"incidentID":     incident.ID,
			"serviceID":      incident.Service.ID,
			"incidentUrl":    incident.HTMLURL,
			"incidentNumber": uintToString(incident.IncidentNumber),
			"title":          incident.Title,
			"status":         incident.Status,
			"urgency":        incident.Urgency,
			"acknowledged":   boolToString(len(incident.Acknowledgements) >= 1),
			"assigned":       boolToString(len(incident.Assignments) >= 1),
			"type":           incident.Type,
			"time":           createdAt.Format(Opts.PagerDuty.Incident.TimeFormat),
		}, createdAt)
	}

	// Track acknowledgements
	for _, acknowledgement := range incident.Acknowledgements {
		ackAt, _ := time.Parse(time.RFC3339, acknowledgement.At)
		incidentStatusMetricList.AddTime(prometheus.Labels{
			"incidentID": incident.ID,
			"userID":     acknowledgement.Acknowledger.ID,
			"time":       ackAt.Format(Opts.PagerDuty.Incident.TimeFormat),
			"type":       "acknowledgement",
		}, ackAt)
	}

	// Track assignments
	for _, assignment := range incident.Assignments {
		assignAt, _ := time.Parse(time.RFC3339, assignment.At)
		incidentStatusMetricList.AddTime(prometheus.Labels{
			"incidentID": incident.ID,
			"userID":     assignment.Assignee.ID,
			"time":       assignAt.Format(Opts.PagerDuty.Incident.TimeFormat),
			"type":       "assignment",
		}, assignAt)
	}

	// Track last status change
	changedAt, _ := time.Parse(time.RFC3339, incident.LastStatusChangeAt)
	incidentStatusMetricList.AddTime(prometheus.Labels{
		"incidentID": incident.ID,
		"userID":     incident.LastStatusChangeBy.ID,
		"time":       changedAt.Format(Opts.PagerDuty.Incident.TimeFormat),
		"type":       "lastChange",
	}, changedAt)

	// Calculate MTTA and MTTR only for filtered incidents
	if !shouldReport {
		return
	}

	serviceName := m.getCachedServiceName(incident.Service.ID, serviceNames)

	// Determine priority
	priority := ""
	if incident.Priority != nil {
		priority = incident.Priority.Name
	}

	// Calculate MTTA
	acknowledgedAt, acknowledgerID := m.getFirstAcknowledgement(incident)
	if !acknowledgedAt.IsZero() {
		mttaSeconds := acknowledgedAt.Sub(createdAt).Seconds()

		incidentMTTAMetricList.Add(prometheus.Labels{
			"incidentID":     incident.ID,
			"serviceID":      incident.Service.ID,
			"serviceName":    serviceName,
			"urgency":        incident.Urgency,
			"acknowledgerID": acknowledgerID,
		}, mttaSeconds)

		serviceKey := incident.Service.ID + "_" + incident.Urgency
		serviceMTTAData[serviceKey] = append(serviceMTTAData[serviceKey], mttaSeconds)

		// P1-specific MTTA
		if priority == "P1" {
			incidentMTTAP1MetricList.Add(prometheus.Labels{
				"incidentID":     incident.ID,
				"serviceID":      incident.Service.ID,
				"serviceName":    serviceName,
				"urgency":        incident.Urgency,
				"acknowledgerID": acknowledgerID,
			}, mttaSeconds)
			serviceMTTAP1Data[serviceKey] = append(serviceMTTAP1Data[serviceKey], mttaSeconds)
		}

		// P2-specific MTTA
		if priority == "P2" {
			incidentMTTAP2MetricList.Add(prometheus.Labels{
				"incidentID":     incident.ID,
				"serviceID":      incident.Service.ID,
				"serviceName":    serviceName,
				"urgency":        incident.Urgency,
				"acknowledgerID": acknowledgerID,
			}, mttaSeconds)
			serviceMTTAP2Data[serviceKey] = append(serviceMTTAP2Data[serviceKey], mttaSeconds)
		}

		// Corporate HQ MTTA
		isCorporateHQ := isCorporateHQIncident(incident.Title)
		if isCorporateHQ {
			incidentMTTACorporateHQMetricList.Add(prometheus.Labels{
				"incidentID":     incident.ID,
				"serviceID":      incident.Service.ID,
				"serviceName":    serviceName,
				"urgency":        incident.Urgency,
				"acknowledgerID": acknowledgerID,
			}, mttaSeconds)
			serviceMTTACorporateHQData[serviceKey] = append(serviceMTTACorporateHQData[serviceKey], mttaSeconds)
		}

		// P1 without Corporate HQ MTTA
		if priority == "P1" && !isCorporateHQ {
			incidentMTTAP1NoCorporateHQMetricList.Add(prometheus.Labels{
				"incidentID":     incident.ID,
				"serviceID":      incident.Service.ID,
				"serviceName":    serviceName,
				"urgency":        incident.Urgency,
				"acknowledgerID": acknowledgerID,
			}, mttaSeconds)
			serviceMTTAP1NoCorporateHQData[serviceKey] = append(serviceMTTAP1NoCorporateHQData[serviceKey], mttaSeconds)
		}
	}

	// Calculate MTTR for resolved incidents
	if incident.Status == "resolved" && incident.LastStatusChangeAt != "" {
		resolvedAt, err := time.Parse(time.RFC3339, incident.LastStatusChangeAt)
		if err == nil {
			mttrSeconds := resolvedAt.Sub(createdAt).Seconds()

			resolverID := incident.LastStatusChangeBy.ID

			incidentMTTRMetricList.Add(prometheus.Labels{
				"incidentID":  incident.ID,
				"serviceID":   incident.Service.ID,
				"serviceName": serviceName,
				"urgency":     incident.Urgency,
				"resolverID":  resolverID,
				"priority":    priority,
			}, mttrSeconds)

			serviceKey := incident.Service.ID + "|" + incident.Urgency + "|" + priority
			serviceMTTRData[serviceKey] = append(serviceMTTRData[serviceKey], mttrSeconds)

			// P1-specific MTTR
			if priority == "P1" {
				incidentMTTRP1MetricList.Add(prometheus.Labels{
					"incidentID":  incident.ID,
					"serviceID":   incident.Service.ID,
					"serviceName": serviceName,
					"urgency":     incident.Urgency,
					"resolverID":  resolverID,
				}, mttrSeconds)
				serviceKeyP1 := incident.Service.ID + "_" + incident.Urgency
				serviceMTTRP1Data[serviceKeyP1] = append(serviceMTTRP1Data[serviceKeyP1], mttrSeconds)
			}

			// P2-specific MTTR
			if priority == "P2" {
				incidentMTTRP2MetricList.Add(prometheus.Labels{
					"incidentID":  incident.ID,
					"serviceID":   incident.Service.ID,
					"serviceName": serviceName,
					"urgency":     incident.Urgency,
					"resolverID":  resolverID,
				}, mttrSeconds)
				serviceKeyP2 := incident.Service.ID + "_" + incident.Urgency
				serviceMTTRP2Data[serviceKeyP2] = append(serviceMTTRP2Data[serviceKeyP2], mttrSeconds)
			}

			// Corporate HQ MTTR
			isCorporateHQ := isCorporateHQIncident(incident.Title)
			if isCorporateHQ {
				incidentMTTRCorporateHQMetricList.Add(prometheus.Labels{
					"incidentID":  incident.ID,
					"serviceID":   incident.Service.ID,
					"serviceName": serviceName,
					"urgency":     incident.Urgency,
					"resolverID":  resolverID,
				}, mttrSeconds)
				serviceKeyCHQ := incident.Service.ID + "_" + incident.Urgency
				serviceMTTRCorporateHQData[serviceKeyCHQ] = append(serviceMTTRCorporateHQData[serviceKeyCHQ], mttrSeconds)
			}

			// P1 without Corporate HQ MTTR
			if priority == "P1" && !isCorporateHQ {
				incidentMTTRP1NoCorporateHQMetricList.Add(prometheus.Labels{
					"incidentID":  incident.ID,
					"serviceID":   incident.Service.ID,
					"serviceName": serviceName,
					"urgency":     incident.Urgency,
					"resolverID":  resolverID,
				}, mttrSeconds)
				serviceKeyP1NoCHQ := incident.Service.ID + "_" + incident.Urgency
				serviceMTTRP1NoCorporateHQData[serviceKeyP1NoCHQ] = append(serviceMTTRP1NoCorporateHQData[serviceKeyP1NoCHQ], mttrSeconds)
			}
		}
	}
}

// getFirstAcknowledgement returns the earliest acknowledgement time and acknowledger ID
func (m *MetricsCollectorIncident) getFirstAcknowledgement(incident pagerduty.Incident) (time.Time, string) {
	var acknowledgedAt time.Time
	var acknowledgerID string

	// Try incident.Acknowledgements first
	for _, ack := range incident.Acknowledgements {
		ackTime, err := time.Parse(time.RFC3339, ack.At)
		if err != nil {
			continue
		}
		if acknowledgedAt.IsZero() || ackTime.Before(acknowledgedAt) {
			acknowledgedAt = ackTime
			acknowledgerID = ack.Acknowledger.ID
		}
	}

	// If empty, fetch from log entries (for resolved incidents)
	if acknowledgedAt.IsZero() {
		logEntries, err := PagerDutyClient.ListIncidentLogEntriesWithContext(m.Context(), incident.ID, pagerduty.ListIncidentLogEntriesOptions{
			Limit:      PagerdutyListLimit,
			IsOverview: true,
		})
		if err == nil {
			for _, entry := range logEntries.LogEntries {
				if strings.HasPrefix(entry.Type, "acknowledge_log_entry") {
					at, err := time.Parse(time.RFC3339, entry.CreatedAt)
					if err != nil {
						continue
					}
					if acknowledgedAt.IsZero() || at.Before(acknowledgedAt) {
						acknowledgedAt = at
						if entry.Agent.ID != "" {
							acknowledgerID = entry.Agent.ID
						}
					}
				}
			}
		}
	}

	return acknowledgedAt, acknowledgerID
}

func (m *MetricsCollectorIncident) getCachedServiceName(serviceID string, cache map[string]string) string {
	if name, exists := cache[serviceID]; exists {
		return name
	}
	name := m.getServiceName(serviceID)
	cache[serviceID] = name
	return name
}

func (m *MetricsCollectorIncident) getServiceName(serviceID string) string {
	service, err := PagerDutyClient.GetServiceWithContext(m.Context(), serviceID, &pagerduty.GetServiceOptions{})
	PrometheusPagerDutyApiCounter.WithLabelValues("GetService").Inc()

	if err != nil {
		return ""
	}
	return service.Name
}

// shouldReportIncident filters incidents:
// 1. Service summary must start with "CDCE"
// 2. Escalation policy summary must NOT end with "DT alerts"
// 3. Priority must be "P1" or "P2"
func (m *MetricsCollectorIncident) shouldReportIncident(incident pagerduty.Incident) bool {
	serviceSummary := m.getServiceSummary(incident)
	if !strings.HasPrefix(serviceSummary, "CDCE") {
		return false
	}

	escalationPolicySummary := m.getEscalationPolicySummary(incident)
	if escalationPolicySummary == "" || strings.HasSuffix(escalationPolicySummary, "DT alerts") {
		return false
	}

	if incident.Priority == nil || (incident.Priority.Name != "P1" && incident.Priority.Name != "P2") {
		return false
	}

	return true
}

func (m *MetricsCollectorIncident) getServiceSummary(incident pagerduty.Incident) string {
	if incident.Service.Summary != "" {
		return incident.Service.Summary
	}

	service, err := PagerDutyClient.GetServiceWithContext(m.Context(), incident.Service.ID, &pagerduty.GetServiceOptions{})
	PrometheusPagerDutyApiCounter.WithLabelValues("GetService").Inc()

	if err != nil {
		return ""
	}
	return service.Name
}

func (m *MetricsCollectorIncident) getEscalationPolicySummary(incident pagerduty.Incident) string {
	if incident.EscalationPolicy.Summary != "" {
		return incident.EscalationPolicy.Summary
	}

	ep, err := PagerDutyClient.GetEscalationPolicyWithContext(m.Context(), incident.EscalationPolicy.ID, &pagerduty.GetEscalationPolicyOptions{})
	PrometheusPagerDutyApiCounter.WithLabelValues("GetEscalationPolicy").Inc()

	if err != nil {
		return ""
	}
	return ep.Name
}

// isCorporateHQIncident checks if the incident title contains Corporate HQ keywords
// Keywords checked (case-insensitive): "corporate hq", "24hundred", "corporatehq"
func isCorporateHQIncident(title string) bool {
	lowerTitle := strings.ToLower(title)
	return strings.Contains(lowerTitle, "corporate hq") ||
		strings.Contains(lowerTitle, "24hundred") ||
		strings.Contains(lowerTitle, "corporatehq")
}

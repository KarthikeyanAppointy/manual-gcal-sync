package main

import (
	"github.com/golang/protobuf/ptypes/timestamp"
	types "go.saastack.io/protos/types"
	"time"
)

type AppointmentWithParentInfo struct {
	AppointmentId string
	ServiceId     string
	TimeSlot      *types.Timeslot
	Quantity      int32
	Status        int32
	RecurringId   string
	Parent        string
}

func (s *Script) ListAppointment(parent string, limit, offset int) ([]*AppointmentWithParentInfo, bool, error) {
	//Todo: add a script to fetch all non recurring appointments in the future from the database
	rows, err := s.DB.Query(`
		select 
		    appointment.id, service_id, time_slot_start_time, time_slot_end_time, appointment.quantity, appointment.status
		from appointy_appointment_v1.appointment
		where parent = $1 and
      			recurring_id = '' and
      			appointment.time_slot_start_time >= now() 
      	limit $2 offset $3;`, parent, limit, offset)
	if err != nil {
		return nil, false, err
	}

	defer rows.Close()

	count := 0

	var appointments []*AppointmentWithParentInfo
	for rows.Next() {
		apt := &AppointmentWithParentInfo{}
		var startTime time.Time
		var endTime time.Time
		if err := rows.Scan(&apt.AppointmentId, &apt.ServiceId, &startTime, &endTime, &apt.Quantity, &apt.Status); err != nil {
			return nil, false, err
		}

		apt.TimeSlot = &types.Timeslot{
			StartTime: &timestamp.Timestamp{Seconds: int64(startTime.Unix())},
			EndTime:   &timestamp.Timestamp{Seconds: int64(endTime.Unix())},
		}
		count = count + 1
		appointments = append(appointments, apt)
	}
	if rows.Err() != nil {
		return nil, false, rows.Err()
	}

	if count < limit {
		return appointments, false, nil
	}

	return appointments, true, nil
}

func (s *Script) ListRecurringAppointment(parent string, limit, offset int) ([]*AppointmentWithParentInfo, bool, error) {
	//Todo: add a script to fetch all non recurring - starting appointments in the future from the database
	rows, err := s.DB.Query(`SELECT DISTINCT ON (recurring_id)
    appointment.id, service_id, time_slot_start_time, time_slot_end_time, appointment.quantity, appointment.status, recurring_id
FROM appointy_appointment_v1.appointment
WHERE parent = $1
    AND recurring_id != ''
    AND time_slot_start_time >= now()
ORDER BY recurring_id, appointment.time_slot_start_time ASC
limit $2 offset $3;`, parent, limit, offset)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	count := 0

	var appointments []*AppointmentWithParentInfo
	for rows.Next() {
		apt := &AppointmentWithParentInfo{}
		var startTime time.Time
		var endTime time.Time
		if err := rows.Scan(&apt.AppointmentId, &apt.ServiceId, &startTime, &endTime, &apt.Quantity, &apt.Status, &apt.RecurringId); err != nil {
			return nil, false, err
		}

		apt.TimeSlot = &types.Timeslot{
			StartTime: &timestamp.Timestamp{Seconds: int64(startTime.Unix())},
			EndTime:   &timestamp.Timestamp{Seconds: int64(endTime.Unix())},
		}
		count = count + 1
		appointments = append(appointments, apt)
	}
	if rows.Err() != nil {
		return nil, false, rows.Err()
	}

	if count < limit {
		return appointments, false, nil
	}

	return appointments, true, nil
}

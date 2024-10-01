package main

import (
	"context"
	"fmt"
	"go.appointy.com/waqt/appointment/pb"
	"google.golang.org/genproto/protobuf/field_mask"
	"strings"
	"time"
)

func (s *Script) UpdateAppointment(appointments []*AppointmentWithParentInfo) error {

	for _, appointment := range appointments {

		updateAppointmentRequest := &pb.UpdateAppointmentRequest{
			Appointment: &pb.Appointment{
				Id:       fmt.Sprintf("%s/%s", parent, appointment.AppointmentId),
				Service:  &pb.Service{Id: appointment.ServiceId},
				TimeSlot: appointment.TimeSlot,
				Quantity: int64(appointment.Quantity),
				Status:   pb.AppointmentStatus(appointment.Status),
			},
			UpdateMask:       &field_mask.FieldMask{Paths: []string{"service", "time_slot", "quantity"}},
			SkipValidation:   true,
			SendNotification: &pb.SendNotification{Email: false, Sms: false},
		}

		_, err := s.AppointmentsClient.UpdateAppointment(context.Background(), updateAppointmentRequest)
		if err != nil {
			if strings.Contains(err.Error(), "nothing to update") {
				return err
			}
			time.Sleep(2 * time.Second)
		}
	}

	return nil
}

func (s *Script) UpdateRecurringAppointment(appointments []*AppointmentWithParentInfo) error {

	for _, appointment := range appointments {
		recurringAptReq := &pb.UpdateRecurringAppointmentsRequest{
			Parent: parent,
			Base: &pb.Appointment{
				Id:       fmt.Sprintf("%s/%s", parent, appointment.AppointmentId),
				Service:  &pb.Service{Id: appointment.ServiceId},
				TimeSlot: appointment.TimeSlot,
				Quantity: int64(appointment.Quantity),
				Status:   pb.AppointmentStatus(appointment.Status),
			},
			UpdateMask:       &field_mask.FieldMask{Paths: []string{"service", "time_slot", "quantity"}},
			RecurringType:    pb.RecurringUpdateType_ThisAndFollowingAppointment,
			RecurringId:      appointment.RecurringId,
			SendNotification: &pb.SendNotification{Email: false, Sms: false},
			SkipValidation:   true,
		}
		_, err := s.AppointmentsClient.UpdateRecurringAppointments(context.Background(), recurringAptReq)
		if err != nil {
			if strings.Contains(err.Error(), "nothing to update") {
				return err
			}
			time.Sleep(2 * time.Second)
		}
	}

	return nil
}

package main

import (
	"context"
	helper "go.appointy.com/waqt/appointment/availability"
	"go.appointy.com/waqt/appointment/pb"
	"strings"
	"time"
)

func (s *Script) UpdateAppointment(appointments []*AppointmentWithParentInfo) error {

	for _, appointment := range appointments {

		_, err := s.AppointmentsClient.UpdateAppointment(context.Background(), &pb.UpdateAppointmentRequest{
			Appointment: &pb.Appointment{
				Id:       appointment.AppointmentId,
				Service:  &pb.Service{Id: appointment.ServiceId},
				TimeSlot: appointment.TimeSlot,
				Quantity: int64(appointment.Quantity),
				Status:   pb.AppointmentStatus(appointment.Status),
			},
			UpdateMask:       helper.FieldMask("service", "timeSlot", "quantity"),
			SkipValidation:   true,
			SendNotification: &pb.SendNotification{Email: false, Sms: false},
		})
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
		_, err := s.AppointmentsClient.UpdateRecurringAppointments(context.Background(), &pb.UpdateRecurringAppointmentsRequest{
			Parent: appointment.Parent,
			Base: &pb.Appointment{
				Id:       appointment.AppointmentId,
				Service:  &pb.Service{Id: appointment.ServiceId},
				TimeSlot: appointment.TimeSlot,
				Quantity: int64(appointment.Quantity),
				Status:   pb.AppointmentStatus(appointment.Status),
			},
			UpdateMask:       helper.FieldMask("service", "timeSlot", "quantity"),
			RecurringType:    pb.RecurringUpdateType_ThisAndFollowingAppointment,
			RecurringId:      appointment.RecurringId,
			SendNotification: &pb.SendNotification{Email: false, Sms: false},
			SkipValidation:   true,
		})
		if err != nil {
			if strings.Contains(err.Error(), "nothing to update") {
				return err
			}
			time.Sleep(2 * time.Second)
		}
	}

	return nil
}
